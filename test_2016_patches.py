from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterable

APP_ID = "471710"
DEPOT_ID = "471711"
MANIFEST_PATCH_RE = re.compile(r"^manifest/(\d+)/Patch\.json$")
RECNET_RE = re.compile(r"rec\s*net", re.IGNORECASE)


class PatchError(RuntimeError):
    pass


class TestFailure(RuntimeError):
    pass


def log(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test changed 2016 Rec Room manifest patches in GitHub Actions.")
    parser.add_argument("--depotdownloader", required=True, help="Path to DepotDownloader.exe")
    parser.add_argument("--steam-username", required=True, help="Steam username")
    parser.add_argument("--steam-password", required=True, help="Steam password")
    parser.add_argument("--run-seconds", type=int, default=120, help="How long to run Recroom_Release.exe")
    parser.add_argument("--repo-root", default=os.getcwd(), help="Repository root")
    parser.add_argument(
        "--work-root",
        default=str(Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())) / "rr-patch-work"),
        help="Temporary working root",
    )
    parser.add_argument(
        "--artifacts-dir",
        default=str(Path(os.getcwd()) / "artifacts"),
        help="Directory where output logs should be copied for artifact upload",
    )
    parser.add_argument(
        "--event-path",
        default=os.environ.get("GITHUB_EVENT_PATH", ""),
        help="Path to the GitHub event JSON payload",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    repo_root = Path(args.repo_root).resolve()
    work_root = Path(args.work_root).resolve()
    artifacts_dir = Path(args.artifacts_dir).resolve()
    depotdownloader = Path(args.depotdownloader).resolve()
    event_path = Path(args.event_path).resolve() if args.event_path else None

    if not depotdownloader.is_file():
        raise FileNotFoundError(f"DepotDownloader.exe was not found: {depotdownloader}")

    readme_path = repo_root / "README.md"
    helper_patch_path = repo_root / "HelperPatches" / "PATCHPC2016.json"

    if not readme_path.is_file():
        raise FileNotFoundError(f"README.md was not found: {readme_path}")
    if not helper_patch_path.is_file():
        raise FileNotFoundError(f"Helper patch was not found: {helper_patch_path}")
    if event_path is None or not event_path.is_file():
        raise FileNotFoundError("GITHUB_EVENT_PATH was not set or the event payload file does not exist.")

    manifests_2016 = parse_2016_manifest_ids(readme_path)
    changed_patch_paths = get_changed_manifest_patch_paths(event_path)

    log(f"[INFO] Loaded {len(manifests_2016)} manifest IDs from the 2016 table in README.md")
    if changed_patch_paths:
        for path in changed_patch_paths:
            log(f"[INFO] Changed patch path: {path}")
    else:
        log("[INFO] No manifest Patch.json changes were found in the push payload.")

    targets: list[tuple[str, Path]] = []
    for relative_path in changed_patch_paths:
        match = MANIFEST_PATCH_RE.match(relative_path)
        if not match:
            continue

        manifest_id = match.group(1)
        patch_path = repo_root / relative_path

        if not patch_path.is_file():
            log(f"[INFO] Skipping {relative_path} because the file does not exist in the checked-out commit.")
            continue
        if manifest_id not in manifests_2016:
            log(f"[INFO] Skipping manifest {manifest_id} because it is not listed in the 2016 table.")
            continue

        targets.append((manifest_id, patch_path))

    if not targets:
        log("[INFO] No changed Patch.json files matched an existing 2016 manifest entry. Nothing to test.")
        return 0

    artifacts_dir.mkdir(parents=True, exist_ok=True)
    work_root.mkdir(parents=True, exist_ok=True)

    failures: list[str] = []

    for manifest_id, patch_path in targets:
        try:
            run_single_manifest_test(
                manifest_id=manifest_id,
                manifest_patch_path=patch_path,
                helper_patch_path=helper_patch_path,
                depotdownloader=depotdownloader,
                steam_username=args.steam_username,
                steam_password=args.steam_password,
                run_seconds=args.run_seconds,
                work_root=work_root,
                artifacts_dir=artifacts_dir,
            )
        except Exception as exc:  # noqa: BLE001
            message = f"Manifest {manifest_id} failed: {exc}"
            failures.append(message)
            log(f"[ERROR] {message}")

    if failures:
        log("[ERROR] One or more manifest tests failed:")
        for failure in failures:
            log(f"[ERROR] - {failure}")
        return 1

    log("[INFO] All changed 2016 manifest patches passed.")
    return 0


def parse_2016_manifest_ids(readme_path: Path) -> set[str]:
    text = readme_path.read_text(encoding="utf-8")
    section_match = re.search(r"(?ms)^# 2016\s*$\n(.*?)^# 2017\s*$", text)
    if not section_match:
        raise PatchError("Could not find the 2016 section in README.md")

    manifest_ids = set(re.findall(r"`(\d+)`", section_match.group(1)))
    if not manifest_ids:
        raise PatchError("No 2016 manifest IDs were found in README.md")
    return manifest_ids


def get_changed_manifest_patch_paths(event_path: Path) -> list[str]:
    payload = json.loads(event_path.read_text(encoding="utf-8"))
    seen: set[str] = set()
    ordered_paths: list[str] = []

    for commit in payload.get("commits", []):
        for key in ("added", "modified", "removed"):
            for path in commit.get(key, []):
                if not MANIFEST_PATCH_RE.match(path):
                    continue
                if path in seen:
                    continue
                seen.add(path)
                ordered_paths.append(path)

    return ordered_paths


def run_single_manifest_test(
    *,
    manifest_id: str,
    manifest_patch_path: Path,
    helper_patch_path: Path,
    depotdownloader: Path,
    steam_username: str,
    steam_password: str,
    run_seconds: int,
    work_root: Path,
    artifacts_dir: Path,
) -> None:
    log(f"[INFO] ===== Testing manifest {manifest_id} =====")
    log(f"[INFO] Using manifest patch: {manifest_patch_path}")

    manifest_work_dir = work_root / manifest_id
    if manifest_work_dir.exists():
        shutil.rmtree(manifest_work_dir)
    manifest_work_dir.mkdir(parents=True, exist_ok=True)

    run_depotdownloader(
        depotdownloader=depotdownloader,
        manifest_id=manifest_id,
        steam_username=steam_username,
        steam_password=steam_password,
        cwd=manifest_work_dir,
    )

    build_root = manifest_work_dir / "depots" / DEPOT_ID / manifest_id
    exe_path = build_root / "Recroom_Release.exe"
    if not exe_path.is_file():
        raise TestFailure(f"Recroom_Release.exe was not found at expected path: {exe_path}")

    log(f"[INFO] Build root: {build_root}")
    log(f"[INFO] Applying helper patch: {helper_patch_path}")
    apply_patch_file(build_root, helper_patch_path)
    log(f"[INFO] Applying manifest patch: {manifest_patch_path}")
    apply_patch_file(build_root, manifest_patch_path)

    output_log_path = build_root / "Recroom_Release_Data" / "output_log.txt"
    run_game_for_duration(exe_path=exe_path, run_seconds=run_seconds)

    if not output_log_path.is_file():
        raise TestFailure(f"Unity output log was not found at expected path: {output_log_path}")

    artifact_log_path = artifacts_dir / f"{manifest_id}-output_log.txt"
    shutil.copy2(output_log_path, artifact_log_path)
    log(f"[INFO] Copied Unity output log to {artifact_log_path}")

    output_text = output_log_path.read_text(encoding="utf-8", errors="ignore")
    if RECNET_RE.search(output_text):
        raise TestFailure("Unity output log contains 'RECNET' or 'Rec Net'.")

    log(f"[INFO] Manifest {manifest_id} passed. No RecNet references were found in output_log.txt")


def run_depotdownloader(
    *,
    depotdownloader: Path,
    manifest_id: str,
    steam_username: str,
    steam_password: str,
    cwd: Path,
) -> None:
    command = [
        str(depotdownloader),
        "-app",
        APP_ID,
        "-depot",
        DEPOT_ID,
        "-manifest",
        manifest_id,
        "-username",
        steam_username,
        "-password",
        steam_password,
    ]

    redacted = [
        str(depotdownloader),
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

    log(f"[INFO] Running DepotDownloader: {' '.join(redacted)}")
    run_streamed_command(command, cwd=cwd)


def run_streamed_command(command: list[str], cwd: Path) -> None:
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    assert process.stdout is not None
    with process.stdout:
        for line in process.stdout:
            print(line.rstrip(), flush=True)

    exit_code = process.wait()
    if exit_code != 0:
        raise TestFailure(f"Command failed with exit code {exit_code}: {command[0]}")


def run_game_for_duration(*, exe_path: Path, run_seconds: int) -> None:
    log(f"[INFO] Launching {exe_path.name} for {run_seconds} seconds")
    process = subprocess.Popen([str(exe_path)], cwd=str(exe_path.parent))
    deadline = time.monotonic() + run_seconds

    try:
        while time.monotonic() < deadline:
            exit_code = process.poll()
            if exit_code is not None:
                log(f"[INFO] Game process exited before timeout with code {exit_code}")
                return
            time.sleep(1)
    finally:
        if process.poll() is None:
            log("[INFO] Stopping Recroom_Release.exe after the requested runtime")
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                log("[INFO] Process did not stop after terminate(); killing it")
                process.kill()
                process.wait(timeout=15)


def apply_patch_file(build_root: Path, patch_path: Path) -> None:
    patch_data = json.loads(patch_path.read_text(encoding="utf-8"))
    instructions = patch_data.get("instructions")
    if not isinstance(instructions, list):
        raise PatchError(f"Patch file has no valid instructions list: {patch_path}")

    for index, instruction in enumerate(instructions, start=1):
        instruction_type = instruction.get("type")
        log(f"[INFO] Applying instruction {index}/{len(instructions)}: {instruction_type}")

        if instruction_type == "replace_bytes":
            apply_replace_bytes_instruction(build_root, instruction)
            continue
        if instruction_type == "write_text_file":
            apply_write_text_file_instruction(build_root, instruction)
            continue

        raise PatchError(f"Unsupported instruction type '{instruction_type}' in {patch_path}")


def apply_replace_bytes_instruction(build_root: Path, instruction: dict) -> None:
    relative_file = instruction.get("file")
    if not relative_file:
        raise PatchError("replace_bytes instruction is missing 'file'")

    target_path = build_root / relative_file
    if not target_path.is_file():
        raise PatchError(f"Patch target file does not exist: {target_path}")

    data = target_path.read_bytes()
    replacements = instruction.get("replacements")
    if not isinstance(replacements, list) or not replacements:
        raise PatchError(f"replace_bytes instruction has no replacements for {target_path}")

    for replacement in replacements:
        find_bytes, replace_bytes = build_replacement_bytes(replacement)
        count = data.count(find_bytes)
        if count == 0:
            raise PatchError(f"Did not find requested bytes in {target_path}")

        expected_count = replacement.get("expected_count")
        if expected_count is not None and count != expected_count:
            raise PatchError(
                f"Expected {expected_count} match(es) in {target_path}, but found {count}"
            )

        data = data.replace(find_bytes, replace_bytes)
        log(f"[INFO] Replaced {count} occurrence(s) in {target_path}")

    target_path.write_bytes(data)


def build_replacement_bytes(replacement: dict) -> tuple[bytes, bytes]:
    if "find_hex" in replacement and "replace_hex" in replacement:
        return bytes.fromhex(replacement["find_hex"]), bytes.fromhex(replacement["replace_hex"])
    if "find" in replacement and "replace" in replacement:
        return replacement["find"].encode("utf-8"), replacement["replace"].encode("utf-8")

    raise PatchError("Replacement must contain either find/replace or find_hex/replace_hex")


def apply_write_text_file_instruction(build_root: Path, instruction: dict) -> None:
    relative_file = instruction.get("file")
    if not relative_file:
        raise PatchError("write_text_file instruction is missing 'file'")

    content = instruction.get("content", "")
    target_path = build_root / relative_file
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(str(content), encoding="utf-8")
    log(f"[INFO] Wrote text file: {target_path}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise
    except Exception as exc:  # noqa: BLE001
        log(f"[ERROR] {exc}")
        raise SystemExit(1)
