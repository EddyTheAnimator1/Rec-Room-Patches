from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

APP_ID = "471710"
DEPOT_ID = "471711"
RECNET_RE = re.compile(r"rec\s*net", re.IGNORECASE)
MANIFEST_PATCH_RE = re.compile(r"^manifest/(?P<manifest_id>\d+)/Patch\.json$")


class PatchTestError(RuntimeError):
    pass


@dataclass(frozen=True)
class CaseResult:
    manifest_id: str
    patch_path: str
    passed: bool
    message: str
    artifact_dir: Path
    output_log_artifact: Path


def log(message: str) -> None:
    print(message, flush=True)


def gh_notice(message: str) -> None:
    print(f"::notice::{message}", flush=True)


def gh_error(message: str) -> None:
    print(f"::error::{message}", flush=True)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test 2016 Rec Room patch uploads.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--work-root", required=True)
    parser.add_argument("--artifacts-root", required=True)
    parser.add_argument("--patchpc-path", required=True)
    parser.add_argument("--depotdownloader-exe", required=True)
    parser.add_argument("--run-seconds", type=int, default=120)
    parser.add_argument("--patch-path")
    parser.add_argument("--event-path")
    return parser.parse_args()



def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise PatchTestError(f"Missing required environment variable: {name}")
    return value



def normalize_repo_rel(path_text: str) -> str:
    path_text = path_text.replace("\\", "/").strip()
    return path_text.lstrip("./")



def load_2016_manifest_ids(readme_path: Path) -> set[str]:
    if not readme_path.is_file():
        raise PatchTestError(f"README.md was not found: {readme_path}")

    text = readme_path.read_text(encoding="utf-8", errors="replace")
    start = text.find("# 2016")
    end = text.find("# 2017")
    if start == -1 or end == -1 or end <= start:
        raise PatchTestError("Could not locate the 2016 section in README.md")

    section = text[start:end]
    manifest_ids = set(re.findall(r"\|\s*[^|]+\s*\|\s*`(\d+)`\s*\|", section))
    if not manifest_ids:
        raise PatchTestError("No 2016 manifest ids were found in README.md")
    return manifest_ids



def read_push_changed_files(event_path: Path) -> list[str]:
    if not event_path.is_file():
        raise PatchTestError(f"GitHub event payload was not found: {event_path}")

    payload = json.loads(event_path.read_text(encoding="utf-8", errors="replace"))
    changed: list[str] = []

    for commit in payload.get("commits", []):
        for key in ("added", "modified"):
            for entry in commit.get(key, []):
                if isinstance(entry, str):
                    changed.append(normalize_repo_rel(entry))

    head_commit = payload.get("head_commit") or {}
    for key in ("added", "modified"):
        for entry in head_commit.get(key, []):
            if isinstance(entry, str):
                changed.append(normalize_repo_rel(entry))

    seen: set[str] = set()
    ordered: list[str] = []
    for item in changed:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered



def resolve_target_patch_paths(
    repo_root: Path,
    allowed_2016_ids: set[str],
    explicit_patch_path: str | None,
    event_path: Path | None,
) -> list[Path]:
    if explicit_patch_path:
        rel = normalize_repo_rel(explicit_patch_path)
        match = MANIFEST_PATCH_RE.fullmatch(rel)
        if not match:
            raise PatchTestError(f"Explicit patch path is not a manifest Patch.json path: {explicit_patch_path}")
        manifest_id = match.group("manifest_id")
        if manifest_id not in allowed_2016_ids:
            raise PatchTestError(f"Explicit patch path is not a 2016 manifest entry: {explicit_patch_path}")
        patch_path = repo_root / rel
        if not patch_path.is_file():
            raise PatchTestError(f"Explicit patch file was not found: {patch_path}")
        return [patch_path]

    if event_path is None:
        return []

    changed_files = read_push_changed_files(event_path)
    selected: list[Path] = []
    for rel in changed_files:
        match = MANIFEST_PATCH_RE.fullmatch(rel)
        if not match:
            continue
        manifest_id = match.group("manifest_id")
        if manifest_id not in allowed_2016_ids:
            continue
        patch_path = repo_root / rel
        if patch_path.is_file():
            selected.append(patch_path)

    return selected



def ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)



def copy_or_write_placeholder(source: Path, destination: Path, placeholder: str | None = None) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.exists():
        shutil.copy2(source, destination)
        return
    if placeholder is None:
        placeholder = f"Missing file: {source}\n"
    destination.write_text(placeholder, encoding="utf-8", errors="replace")



def apply_patch_file(patch_path: Path, build_root: Path) -> None:
    payload = json.loads(patch_path.read_text(encoding="utf-8", errors="replace"))
    instructions = payload.get("instructions")
    if not isinstance(instructions, list):
        raise PatchTestError(f"Patch file has no valid instructions list: {patch_path}")

    for index, instruction in enumerate(instructions, start=1):
        if not isinstance(instruction, dict):
            raise PatchTestError(f"Instruction #{index} in {patch_path} is not an object")

        instruction_type = instruction.get("type")
        target_rel = instruction.get("file")
        if not isinstance(target_rel, str) or not target_rel.strip():
            raise PatchTestError(f"Instruction #{index} in {patch_path} is missing a target file")

        target_path = build_root / Path(target_rel)

        if instruction_type == "replace_bytes":
            replacements = instruction.get("replacements")
            if not isinstance(replacements, list) or not replacements:
                raise PatchTestError(f"Instruction #{index} in {patch_path} has no replacements")
            if not target_path.is_file():
                raise PatchTestError(f"Patch target was not found: {target_path}")

            data = target_path.read_bytes()
            original_data = data

            for repl_index, replacement in enumerate(replacements, start=1):
                if not isinstance(replacement, dict):
                    raise PatchTestError(
                        f"Replacement #{repl_index} in instruction #{index} of {patch_path} is not an object"
                    )

                if "find_hex" in replacement or "replace_hex" in replacement:
                    try:
                        find_bytes = bytes.fromhex(str(replacement["find_hex"]))
                        replace_bytes = bytes.fromhex(str(replacement["replace_hex"]))
                    except KeyError as exc:
                        raise PatchTestError(
                            f"Replacement #{repl_index} in instruction #{index} of {patch_path} is missing hex keys"
                        ) from exc
                    except ValueError as exc:
                        raise PatchTestError(
                            f"Replacement #{repl_index} in instruction #{index} of {patch_path} has invalid hex"
                        ) from exc
                else:
                    try:
                        find_bytes = str(replacement["find"]).encode("utf-8")
                        replace_bytes = str(replacement["replace"]).encode("utf-8")
                    except KeyError as exc:
                        raise PatchTestError(
                            f"Replacement #{repl_index} in instruction #{index} of {patch_path} is missing text keys"
                        ) from exc

                hit_count = data.count(find_bytes)
                expected_count = replacement.get("expected_count")
                if expected_count is not None:
                    try:
                        expected_count = int(expected_count)
                    except (TypeError, ValueError) as exc:
                        raise PatchTestError(
                            f"Replacement #{repl_index} in instruction #{index} of {patch_path} has invalid expected_count"
                        ) from exc

                if hit_count == 0:
                    raise PatchTestError(
                        f"Replacement #{repl_index} in instruction #{index} of {patch_path} matched 0 times in {target_path}"
                    )
                if expected_count is not None and hit_count != expected_count:
                    raise PatchTestError(
                        f"Replacement #{repl_index} in instruction #{index} of {patch_path} matched {hit_count} times in {target_path}; expected {expected_count}"
                    )

                data = data.replace(find_bytes, replace_bytes)
                log(
                    f"Applied replacement #{repl_index} from {patch_path.name} to {target_rel} ({hit_count} hit(s))"
                )

            if data != original_data:
                target_path.write_bytes(data)
            continue

        if instruction_type == "write_text_file":
            content = instruction.get("content")
            if content is None:
                raise PatchTestError(f"Instruction #{index} in {patch_path} is missing content")
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(str(content), encoding="utf-8", errors="replace")
            log(f"Wrote text file from {patch_path.name} to {target_rel}")
            continue

        raise PatchTestError(f"Unsupported instruction type in {patch_path}: {instruction_type}")



def run_depotdownloader(
    depotdownloader_exe: Path,
    manifest_id: str,
    download_root: Path,
    username: str,
    password: str,
    log_path: Path,
) -> Path:
    download_root.mkdir(parents=True, exist_ok=True)
    command = [
        str(depotdownloader_exe),
        "-app",
        APP_ID,
        "-depot",
        DEPOT_ID,
        "-manifest",
        manifest_id,
        "-username",
        username,
        "-password",
        password,
    ]

    redacted = [
        depotdownloader_exe.name,
        "-app",
        APP_ID,
        "-depot",
        DEPOT_ID,
        "-manifest",
        manifest_id,
        "-username",
        "***",
        "-password",
        "***",
    ]
    log(f"Running DepotDownloader: {' '.join(redacted)}")

    with log_path.open("w", encoding="utf-8", errors="replace") as handle:
        process = subprocess.run(
            command,
            cwd=str(download_root),
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )

    if process.returncode != 0:
        raise PatchTestError(f"DepotDownloader exited with code {process.returncode}")

    build_root = download_root / "depots" / DEPOT_ID / manifest_id
    if not build_root.is_dir():
        raise PatchTestError(f"Downloaded build directory was not found: {build_root}")
    return build_root



def kill_process_tree(pid: int) -> None:
    subprocess.run(
        ["taskkill", "/PID", str(pid), "/T", "/F"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )



def launch_and_capture(
    exe_path: Path,
    run_seconds: int,
    runtime_log_path: Path,
) -> None:
    creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)

    with runtime_log_path.open("w", encoding="utf-8", errors="replace") as handle:
        process = subprocess.Popen(
            [str(exe_path)],
            cwd=str(exe_path.parent),
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=creationflags,
        )

        deadline = time.time() + max(run_seconds, 1)
        while time.time() < deadline:
            if process.poll() is not None:
                break
            time.sleep(1.0)

        if process.poll() is None:
            kill_process_tree(process.pid)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                kill_process_tree(process.pid)
                process.wait(timeout=15)

    time.sleep(5.0)



def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")



def append_summary(lines: Iterable[str]) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "").strip()
    if not summary_path:
        return
    with Path(summary_path).open("a", encoding="utf-8", errors="replace") as handle:
        for line in lines:
            handle.write(f"{line}\n")



def test_one_patch(
    repo_root: Path,
    patchpc_path: Path,
    depotdownloader_exe: Path,
    patch_path: Path,
    work_root: Path,
    artifacts_root: Path,
    run_seconds: int,
    username: str,
    password: str,
) -> CaseResult:
    rel_patch = patch_path.relative_to(repo_root).as_posix()
    match = MANIFEST_PATCH_RE.fullmatch(rel_patch)
    if not match:
        raise PatchTestError(f"Unexpected patch path format: {rel_patch}")
    manifest_id = match.group("manifest_id")

    case_root = work_root / manifest_id
    case_artifacts = artifacts_root / manifest_id
    ensure_clean_dir(case_root)
    ensure_clean_dir(case_artifacts)

    depot_log = case_artifacts / "depotdownloader.log"
    runtime_log = case_artifacts / "runtime_console.log"
    artifact_output_log = case_artifacts / "output_log.txt"

    try:
        build_root = run_depotdownloader(
            depotdownloader_exe=depotdownloader_exe,
            manifest_id=manifest_id,
            download_root=case_root / "download",
            username=username,
            password=password,
            log_path=depot_log,
        )

        exe_path = build_root / "Recroom_Release.exe"
        if not exe_path.is_file():
            raise PatchTestError(f"Recroom_Release.exe was not found: {exe_path}")

        output_log_path = build_root / "Recroom_Release_Data" / "output_log.txt"
        if output_log_path.exists():
            output_log_path.unlink()

        log(f"Applying helper patch: {patchpc_path}")
        apply_patch_file(patchpc_path, build_root)
        log(f"Applying manifest patch: {patch_path}")
        apply_patch_file(patch_path, build_root)

        log(f"Launching build for {run_seconds} second(s): {exe_path}")
        launch_and_capture(exe_path=exe_path, run_seconds=run_seconds, runtime_log_path=runtime_log)

        if not output_log_path.is_file():
            placeholder = f"Unity output log was not found at expected path: {output_log_path}\n"
            artifact_output_log.write_text(placeholder, encoding="utf-8", errors="replace")
            raise PatchTestError(placeholder.strip())

        copy_or_write_placeholder(output_log_path, artifact_output_log)
        output_text = read_text(output_log_path)

        if RECNET_RE.search(output_text):
            raise PatchTestError("Detected RECNET in Recroom_Release_Data/output_log.txt")

        message = "Passed: no RECNET references were found in output_log.txt"
        gh_notice(f"{manifest_id}: {message}")
        return CaseResult(
            manifest_id=manifest_id,
            patch_path=rel_patch,
            passed=True,
            message=message,
            artifact_dir=case_artifacts,
            output_log_artifact=artifact_output_log,
        )

    except PatchTestError as exc:
        if not artifact_output_log.exists():
            copy_or_write_placeholder(
                source=Path("__missing__"),
                destination=artifact_output_log,
                placeholder=str(exc).rstrip() + "\n",
            )
        gh_error(f"{manifest_id}: {exc}")
        return CaseResult(
            manifest_id=manifest_id,
            patch_path=rel_patch,
            passed=False,
            message=str(exc),
            artifact_dir=case_artifacts,
            output_log_artifact=artifact_output_log,
        )



def main() -> int:
    args = parse_args()

    repo_root = Path(args.repo_root).resolve()
    work_root = Path(args.work_root).resolve()
    artifacts_root = Path(args.artifacts_root).resolve()
    patchpc_path = (repo_root / args.patchpc_path).resolve()
    depotdownloader_exe = Path(args.depotdownloader_exe).resolve()
    event_path = Path(args.event_path).resolve() if args.event_path else None

    ensure_clean_dir(work_root)
    artifacts_root.mkdir(parents=True, exist_ok=True)

    if not patchpc_path.is_file():
        raise PatchTestError(f"Helper patch was not found: {patchpc_path}")
    if not depotdownloader_exe.is_file():
        raise PatchTestError(f"DepotDownloader.exe was not found: {depotdownloader_exe}")

    username = require_env("STEAM_USERNAME")
    password = require_env("STEAM_PASSWORD")

    allowed_2016_ids = load_2016_manifest_ids(repo_root / "README.md")
    target_patches = resolve_target_patch_paths(
        repo_root=repo_root,
        allowed_2016_ids=allowed_2016_ids,
        explicit_patch_path=args.patch_path,
        event_path=event_path,
    )

    if not target_patches:
        message = "No eligible 2016 Patch.json changes were detected."
        gh_notice(message)
        append_summary(["# Test 2016 patches", "", f"- {message}"])
        return 0

    log("Selected patch files:")
    for patch_path in target_patches:
        log(f"- {patch_path.relative_to(repo_root).as_posix()}")

    results: list[CaseResult] = []
    for patch_path in target_patches:
        results.append(
            test_one_patch(
                repo_root=repo_root,
                patchpc_path=patchpc_path,
                depotdownloader_exe=depotdownloader_exe,
                patch_path=patch_path,
                work_root=work_root,
                artifacts_root=artifacts_root,
                run_seconds=args.run_seconds,
                username=username,
                password=password,
            )
        )

    summary_lines = ["# Test 2016 patches", ""]
    overall_ok = True
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        summary_lines.append(f"- **{result.manifest_id}** — {status} — `{result.patch_path}` — {result.message}")
        summary_lines.append(f"  - Artifact log: `{result.output_log_artifact.relative_to(artifacts_root).as_posix()}`")
        overall_ok = overall_ok and result.passed
    append_summary(summary_lines)

    summary_file = artifacts_root / "summary.md"
    summary_file.write_text("\n".join(summary_lines) + "\n", encoding="utf-8", errors="replace")

    return 0 if overall_ok else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PatchTestError as exc:
        gh_error(str(exc))
        raise SystemExit(1)
