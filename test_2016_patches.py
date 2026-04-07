from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

APP_ID = 471710
DEPOT_ID = 471711
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
)
DEPOTDOWNLOADER_RELEASE_API = "https://api.github.com/repos/SteamRE/DepotDownloader/releases/latest"
RECNET_RE = re.compile(r"rec\s*net", re.IGNORECASE)
DATE_KEYS = ("date", "label", "name", "title")
EXE_NAME_PREFERENCES = [
    "RecRoom_Release.exe",
    "Recroom_Release.exe",
    "RecRoom.exe",
    "Rec Room.exe",
]
EXE_NAME_BLOCKLIST = {
    "UnityCrashHandler64.exe",
    "UnityCrashHandler32.exe",
    "crashpad_handler.exe",
    "steamerrorreporter.exe",
    "uninstall.exe",
    "unins000.exe",
    "DepotDownloader.exe",
}


class WorkflowError(RuntimeError):
    pass


class PatchError(WorkflowError):
    pass


def log(text: str) -> None:
    print(text, flush=True)


def gh_notice(text: str) -> None:
    print(f"::notice::{text}", flush=True)


def gh_warning(text: str) -> None:
    print(f"::warning::{text}", flush=True)


def gh_error(text: str) -> None:
    print(f"::error::{text}", flush=True)


def request_bytes(url: str, extra_headers: dict[str, str] | None = None) -> bytes:
    headers = {"User-Agent": USER_AGENT}
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as response:
        return response.read()


def request_json(url: str) -> Any:
    return json.loads(
        request_bytes(
            url,
            {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        ).decode("utf-8", errors="replace")
    )


def mask_command(args: list[str]) -> str:
    masked: list[str] = []
    hide_next = False
    sensitive_flags = {"-password", "-username", "-twofactor"}
    for part in args:
        if hide_next:
            masked.append("<hidden>")
            hide_next = False
            continue
        masked.append(part)
        if part in sensitive_flags:
            hide_next = True
    return " ".join(masked)


def run_checked(args: list[str], cwd: Path, env: dict[str, str] | None = None) -> str:
    proc = subprocess.run(
        args,
        cwd=str(cwd),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if proc.returncode != 0:
        raise WorkflowError(
            f"Command failed ({proc.returncode}): {mask_command(args)}\n{proc.stdout}"
        )
    return proc.stdout


def load_github_event() -> dict[str, Any]:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        return {}
    path = Path(event_path)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_secret(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise WorkflowError(f"Missing required secret/env: {name}")
    return value


def choose_release_asset(payload: dict[str, Any]) -> tuple[str, str]:
    assets = payload.get("assets") or []
    scored: list[tuple[int, str, str]] = []

    for asset in assets:
        name = str(asset.get("name", ""))
        url = str(asset.get("browser_download_url", ""))
        if not name or not url or not name.lower().endswith(".zip"):
            continue

        lower = name.lower()
        score = 0
        if "windows" in lower:
            score += 100
        if "win" in lower:
            score += 60
        if "x64" in lower or "amd64" in lower:
            score += 40
        if "portable" in lower:
            score += 10
        if "linux" in lower or "osx" in lower or "mac" in lower:
            score -= 100
        scored.append((score, name, url))

    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    if not scored or scored[0][0] <= 0:
        raise WorkflowError("Could not find a Windows DepotDownloader release asset.")
    _, name, url = scored[0]
    return name, url


def ensure_depotdownloader(tools_root: Path) -> Path:
    exe_path = tools_root / "DepotDownloader" / "DepotDownloader.exe"
    if exe_path.exists():
        return exe_path

    tools_root.mkdir(parents=True, exist_ok=True)
    payload = request_json(DEPOTDOWNLOADER_RELEASE_API)
    asset_name, asset_url = choose_release_asset(payload)
    zip_path = tools_root / asset_name
    extract_dir = tools_root / "DepotDownloader"
    temp_dir = tools_root / "DepotDownloader_extract"

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if extract_dir.exists():
        shutil.rmtree(extract_dir)

    log(f"Downloading DepotDownloader: {asset_name}")
    zip_path.write_bytes(request_bytes(asset_url))

    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(temp_dir)

    temp_dir.rename(extract_dir)
    if not exe_path.exists():
        matches = list(extract_dir.rglob("DepotDownloader.exe"))
        if not matches:
            raise WorkflowError("DepotDownloader.exe was not found after extraction.")
        return matches[0]
    return exe_path


def score_executable(path: Path, build_dir: Path) -> tuple[int, int, str]:
    name = path.name.lower()
    score = 0
    for index, preferred in enumerate(EXE_NAME_PREFERENCES):
        if name == preferred.lower():
            score += 200 - index * 10
    if path.parent == build_dir:
        score += 40
    if "recroom" in name or "rec room" in name:
        score += 30
    if "launcher" in name:
        score += 10
    if "unitycrashhandler" in name or "crash" in name:
        score -= 200
    if "unins" in name or "uninstall" in name:
        score -= 200
    if "depotdownloader" in name:
        score -= 500
    return score, len(path.parts), str(path).lower()


def find_launch_executable(build_dir: Path) -> Path:
    candidates = [path for path in build_dir.rglob("*.exe") if path.is_file()]
    filtered = [path for path in candidates if path.name not in EXE_NAME_BLOCKLIST]
    if not filtered:
        filtered = candidates
    if not filtered:
        raise WorkflowError(f"No executable was found under {build_dir}")
    filtered.sort(key=lambda item: score_executable(item, build_dir), reverse=True)
    best = filtered[0]
    if score_executable(best, build_dir)[0] < 0:
        raise WorkflowError(f"Could not choose a valid executable under {build_dir}")
    return best


def parse_date_json(path: Path) -> str:
    if not path.exists():
        return ""
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, str):
        return data.strip()
    if isinstance(data, dict):
        for key in DATE_KEYS:
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def is_2016_patch(patch_path: Path) -> bool:
    date_label = parse_date_json(patch_path.with_name("Date.json"))
    if "2016" in date_label:
        return True
    return "2016" in patch_path.as_posix()


def normalize_patch_instructions(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        if isinstance(payload.get("instructions"), list):
            items = payload["instructions"]
        elif isinstance(payload.get("patches"), list):
            items = payload["patches"]
        else:
            items = [payload]
    else:
        raise PatchError("Patch.json must be a JSON object or list.")

    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            raise PatchError("Each patch instruction must be a JSON object.")

        kind = str(item.get("type") or item.get("kind") or "replace_bytes").strip().lower()
        file_value = item.get("file") or item.get("path")
        if not isinstance(file_value, str) or not file_value.strip():
            raise PatchError("Each patch instruction needs a file/path string.")

        if kind in {"write_text_file", "create_text_file", "write_text"}:
            content = item.get("content")
            if content is None:
                content = item.get("text", item.get("value"))
            if not isinstance(content, str):
                raise PatchError("Text-file patch instructions need string content.")
            normalized.append(
                {
                    "type": kind,
                    "file": file_value.strip(),
                    "content": content,
                    "encoding": str(item.get("encoding") or "utf-8"),
                    "overwrite": bool(item.get("overwrite", True)),
                }
            )
            continue

        replacements = item.get("replacements")
        if replacements is None:
            find_value = item.get("find")
            replace_value = item.get("replace")
            if isinstance(find_value, str) and isinstance(replace_value, str):
                replacements = [{"find": find_value, "replace": replace_value}]
            else:
                raise PatchError("Each patch instruction needs replacements or find/replace values.")

        if not isinstance(replacements, list) or not replacements:
            raise PatchError("replacements must be a non-empty list.")

        normalized.append(
            {
                "type": kind,
                "file": file_value.strip(),
                "replacements": replacements,
                "encoding": str(item.get("encoding") or "utf-8"),
            }
        )

    return normalized


def resolve_target_file(build_root: Path, file_value: str) -> Path:
    direct = build_root / Path(file_value)
    if direct.exists() and direct.is_file():
        return direct

    name = Path(file_value).name
    matches = [path for path in build_root.rglob(name) if path.is_file()]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise PatchError(f"Patch target file was not found: {file_value}")
    raise PatchError(f"Patch target file is ambiguous: {file_value}")


def ensure_backup(path: Path) -> None:
    backup = path.with_name(path.name + ".bak")
    if not backup.exists():
        shutil.copy2(path, backup)


def make_replacement_bytes(replacement: dict[str, Any], encoding: str) -> tuple[bytes, bytes, int | None]:
    expected_count = replacement.get("expected_count")
    if expected_count is not None and not isinstance(expected_count, int):
        raise PatchError("expected_count must be an integer when provided.")

    if isinstance(replacement.get("find_hex"), str) and isinstance(replacement.get("replace_hex"), str):
        try:
            find_bytes = bytes.fromhex(replacement["find_hex"])
            replace_bytes = bytes.fromhex(replacement["replace_hex"])
        except ValueError as exc:
            raise PatchError(f"Invalid hex replacement: {exc}") from exc
    else:
        find_value = replacement.get("find")
        replace_value = replacement.get("replace")
        if not isinstance(find_value, str) or not isinstance(replace_value, str):
            raise PatchError("Replacement needs either find/replace or find_hex/replace_hex.")
        find_bytes = find_value.encode(encoding)
        replace_bytes = replace_value.encode(encoding)

    if len(find_bytes) != len(replace_bytes):
        raise PatchError("Replacement changes byte length. That is blocked.")
    if not find_bytes:
        raise PatchError("Replacement find value is empty.")
    return find_bytes, replace_bytes, expected_count


def apply_replace_bytes(file_path: Path, replacements: list[dict[str, Any]], encoding: str) -> list[str]:
    data = file_path.read_bytes()
    applied: list[str] = []

    for index, replacement in enumerate(replacements, start=1):
        if not isinstance(replacement, dict):
            raise PatchError(f"Replacement #{index} is not a JSON object.")

        find_bytes, replace_bytes, expected_count = make_replacement_bytes(replacement, encoding)
        count = data.count(find_bytes)
        if count <= 0:
            raise PatchError(f"Replacement #{index} could not find its target inside {file_path.name}.")
        if expected_count is not None and count != expected_count:
            raise PatchError(
                f"Replacement #{index} expected {expected_count} match(es) in {file_path.name}, found {count}."
            )

        data = data.replace(find_bytes, replace_bytes)
        applied.append(f"{file_path}: replacement #{index} applied {count} time(s)")

    ensure_backup(file_path)
    file_path.write_bytes(data)
    return applied


def write_text_file(target_path: Path, content: str, encoding: str, overwrite: bool) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and target_path.is_dir():
        raise PatchError(f"Patch target path is a directory, not a file: {target_path}")
    if target_path.exists():
        if not overwrite:
            raise PatchError(f"Patch target file already exists and overwrite is disabled: {target_path.name}")
        ensure_backup(target_path)
    target_path.write_text(content, encoding=encoding, newline="")


def apply_patch_payload(build_root: Path, payload: Any) -> list[str]:
    instructions = normalize_patch_instructions(payload)
    results: list[str] = []

    for instruction in instructions:
        kind = instruction["type"]
        if kind in {"write_text_file", "create_text_file", "write_text"}:
            target_file = build_root / Path(instruction["file"])
            write_text_file(
                target_file,
                instruction["content"],
                instruction["encoding"],
                instruction["overwrite"],
            )
            results.append(f"{target_file}: text file written")
            continue

        if kind not in {"replace_bytes", "replace_text", "replace_strings"}:
            raise PatchError(f"Unsupported patch type: {kind}")

        target_file = resolve_target_file(build_root, instruction["file"])
        results.extend(
            apply_replace_bytes(
                target_file,
                instruction["replacements"],
                instruction["encoding"],
            )
        )

    return results


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def tee_process_to_file(args: list[str], cwd: Path, log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8", errors="replace") as handle:
        process = subprocess.Popen(
            args,
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=None,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            handle.write(line)
            handle.flush()
            print(line.rstrip("\r\n"), flush=True)
        return process.wait()


def kill_process_tree(pid: int) -> None:
    subprocess.run(
        ["taskkill", "/PID", str(pid), "/T", "/F"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def candidate_output_logs(build_root: Path, preferred_log: Path | None = None) -> list[Path]:
    candidates: list[Path] = []

    def add(path: Path) -> None:
        if path not in candidates:
            candidates.append(path)

    if preferred_log is not None:
        add(preferred_log)

    add(build_root / "Recroom_Release_Data" / "output_log.txt")
    add(build_root / "Recroom_Release_Data" / "Player.log")

    for name in ("output_log.txt", "Player.log"):
        for match in build_root.rglob(name):
            add(match)

    local_low = Path(os.environ.get("USERPROFILE", "")) / "AppData" / "LocalLow"
    if local_low.exists():
        for name in ("output_log.txt", "Player.log"):
            for match in local_low.rglob(name):
                add(match)

    return candidates


def find_best_output_log(build_root: Path, preferred_log: Path, started_at: float) -> Path:
    scored: list[tuple[int, float, int, Path]] = []
    for path in candidate_output_logs(build_root, preferred_log):
        if not path.exists() or not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue

        score = 0
        if path == preferred_log:
            score += 10000
        lower = str(path).lower()
        if "locallow" in lower:
            score += 2500
        if "against gravity" in lower:
            score += 1200
        if "rec room" in lower:
            score += 1200
        if "recroom_release_data" in lower:
            score += 800
        if stat.st_mtime >= started_at - 5:
            score += 1500
        if path.name.lower() == "output_log.txt":
            score += 100
        scored.append((score, stat.st_mtime, -len(str(path)), path))

    if not scored:
        return preferred_log

    scored.sort(reverse=True)
    return scored[0][3]


def monitor_run(exe_path: Path, build_root: Path, run_seconds: int, case_dir: Path) -> tuple[bool, str, Path]:
    runtime_log = case_dir / "runtime_console.log"
    live_unity_log = case_dir / "unity_output_log_live.txt"
    final_unity_log = case_dir / "unity_output_log.txt"
    legacy_output_log = case_dir / "output_log.txt"
    combined_log = case_dir / "combined_log.txt"
    forced_log = case_dir / "unity_output_log_capture.txt"

    if forced_log.exists():
        forced_log.unlink()

    started_at = time.time()

    with runtime_log.open("w", encoding="utf-8", errors="replace") as console_handle:
        process = subprocess.Popen(
            [str(exe_path), "-logFile", str(forced_log)],
            cwd=str(exe_path.parent),
            stdout=console_handle,
            stderr=subprocess.STDOUT,
            stdin=None,
        )
        output_log = forced_log
        cursor = 0
        recnet_hit = False
        failure_reason = ""
        end_time = time.monotonic() + run_seconds

        try:
            while time.monotonic() < end_time:
                current_log = find_best_output_log(build_root, forced_log, started_at)
                if current_log != output_log and current_log.exists():
                    output_log = current_log
                    cursor = 0
                    gh_notice(f"Using Unity log file: {output_log}")

                if output_log.exists():
                    with output_log.open("r", encoding="utf-8", errors="replace") as handle:
                        handle.seek(cursor)
                        chunk = handle.read()
                        cursor = handle.tell()
                    if chunk:
                        with live_unity_log.open("a", encoding="utf-8", errors="replace") as mirror:
                            mirror.write(chunk)
                        if RECNET_RE.search(chunk):
                            recnet_hit = True
                            failure_reason = f"RecNet-related error detected in {output_log}"
                            break

                return_code = process.poll()
                if return_code is not None:
                    if return_code != 0:
                        failure_reason = f"Game process exited with code {return_code}"
                        break
                    gh_warning("Game process exited before the full wait period, but no RecNet error was found.")
                    time.sleep(1)
                    continue

                time.sleep(1)
        finally:
            kill_process_tree(process.pid)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                pass

    output_log = find_best_output_log(build_root, forced_log, started_at)

    if output_log.exists():
        shutil.copy2(output_log, final_unity_log)
        shutil.copy2(output_log, legacy_output_log)
    else:
        missing_text = (
            "Unity output log was not found. Tried forced log path and fallback locations.\n"
            f"Preferred forced path: {forced_log}\n"
        )
        final_unity_log.write_text(missing_text, encoding="utf-8")
        legacy_output_log.write_text(missing_text, encoding="utf-8")

    combined_parts: list[str] = []
    combined_parts.append("===== runtime_console.log =====\n")
    if runtime_log.exists():
        combined_parts.append(runtime_log.read_text(encoding="utf-8", errors="replace"))
    else:
        combined_parts.append("<missing>\n")

    combined_parts.append("\n===== unity_output_log.txt =====\n")
    if final_unity_log.exists():
        combined_parts.append(final_unity_log.read_text(encoding="utf-8", errors="replace"))
    else:
        combined_parts.append("<missing>\n")

    combined_log.write_text("".join(combined_parts), encoding="utf-8", errors="replace")

    if recnet_hit:
        return False, failure_reason, output_log
    if failure_reason:
        return False, failure_reason, output_log
    return True, f"No RecNet error found during {run_seconds} seconds.", output_log


def find_build_root(download_root: Path) -> Path:
    exe_path = find_launch_executable(download_root)
    current = exe_path.parent
    while current != download_root and current.parent != current:
        if current.parent == download_root:
            return current
        current = current.parent
    return exe_path.parent


def download_manifest(
    depotdownloader_exe: Path,
    manifest_id: str,
    username: str,
    password: str,
    twofactor: str,
    download_root: Path,
    log_path: Path,
) -> Path:
    if download_root.exists():
        shutil.rmtree(download_root)
    download_root.mkdir(parents=True, exist_ok=True)

    args = [
        str(depotdownloader_exe),
        "-app",
        str(APP_ID),
        "-depot",
        str(DEPOT_ID),
        "-manifest",
        manifest_id,
        "-username",
        username,
        "-password",
        password,
        "-dir",
        str(download_root),
    ]
    if twofactor:
        args.extend(["-twofactor", twofactor])

    log(f"Running: {mask_command(args)}")
    exit_code = tee_process_to_file(args, cwd=download_root, log_path=log_path)
    if exit_code != 0:
        raise WorkflowError(f"DepotDownloader failed with exit code {exit_code}")
    return find_build_root(download_root)


def changed_patch_files(repo_root: Path, manual_patch_path: str | None) -> list[Path]:
    if manual_patch_path:
        path = (repo_root / manual_patch_path).resolve()
        if not path.exists():
            raise WorkflowError(f"Manual patch path does not exist: {manual_patch_path}")
        if path.name != "Patch.json":
            raise WorkflowError(f"Manual patch path must point to a Patch.json file: {manual_patch_path}")
        return [path]

    event = load_github_event()
    event_name = os.environ.get("GITHUB_EVENT_NAME", "")
    head_sha = os.environ.get("GITHUB_SHA", "")

    if event_name == "push":
        base_sha = str(event.get("before") or "").strip()
        target_sha = str(event.get("after") or head_sha).strip()
    elif event_name == "pull_request":
        pr = event.get("pull_request") or {}
        base_sha = str(((pr.get("base") or {}).get("sha")) or "").strip()
        target_sha = str(((pr.get("head") or {}).get("sha")) or head_sha).strip()
    else:
        raise WorkflowError("No patch path was supplied, and this event type is not supported for auto-discovery.")

    if not base_sha or re.fullmatch(r"0+", base_sha):
        files = [path.relative_to(repo_root).as_posix() for path in repo_root.rglob("Patch.json")]
    else:
        output = run_checked(["git", "diff", "--name-only", base_sha, target_sha], cwd=repo_root)
        files = output.splitlines()

    changed: list[Path] = []
    for raw in files:
        raw = raw.strip()
        if not raw:
            continue
        posix_path = PurePosixPath(raw)
        if posix_path.name != "Patch.json":
            continue
        path = (repo_root / Path(raw)).resolve()
        if path.exists():
            changed.append(path)

    unique = sorted({path.as_posix(): path for path in changed}.values(), key=lambda item: item.as_posix())
    return unique


def write_summary(step_summary: Path | None, text: str) -> None:
    if step_summary is None:
        return
    step_summary.parent.mkdir(parents=True, exist_ok=True)
    with step_summary.open("a", encoding="utf-8") as handle:
        handle.write(text)


def test_one_patch(
    repo_root: Path,
    patch_path: Path,
    patchpc_path: Path,
    depotdownloader_exe: Path,
    username: str,
    password: str,
    twofactor: str,
    run_seconds: int,
    artifacts_root: Path,
    work_root: Path,
) -> dict[str, Any]:
    manifest_id = patch_path.parent.name
    case_dir = artifacts_root / manifest_id
    case_dir.mkdir(parents=True, exist_ok=True)

    repo_patch_payload = json.loads(patch_path.read_text(encoding="utf-8"))
    save_json(case_dir / "repo_patch.json", repo_patch_payload)

    is_2016 = is_2016_patch(patch_path)
    if is_2016:
        patchpc_payload = json.loads(patchpc_path.read_text(encoding="utf-8"))
        save_json(case_dir / patchpc_path.name, patchpc_payload)
    else:
        patchpc_payload = None

    download_root = work_root / manifest_id / "download"
    depot_log = case_dir / "depotdownloader.log"

    result: dict[str, Any] = {
        "manifest_id": manifest_id,
        "patch_path": patch_path.relative_to(repo_root).as_posix(),
        "is_2016": is_2016,
        "passed": False,
        "message": "",
    }

    try:
        build_root = download_manifest(
            depotdownloader_exe=depotdownloader_exe,
            manifest_id=manifest_id,
            username=username,
            password=password,
            twofactor=twofactor,
            download_root=download_root,
            log_path=depot_log,
        )
        result["build_root"] = str(build_root)

        patch_log_lines: list[str] = []
        patch_log_lines.extend(apply_patch_payload(build_root, repo_patch_payload))
        if patchpc_payload is not None:
            patch_log_lines.extend(apply_patch_payload(build_root, patchpc_payload))
        (case_dir / "patch_apply.log").write_text("\n".join(patch_log_lines) + "\n", encoding="utf-8")

        exe_path = find_launch_executable(build_root)
        result["exe_path"] = str(exe_path)
        passed, message, output_log = monitor_run(
            exe_path=exe_path,
            build_root=build_root,
            run_seconds=run_seconds,
            case_dir=case_dir,
        )
        result["passed"] = passed
        result["message"] = message
        result["output_log_path"] = str(output_log)
        result["artifact_runtime_console_log"] = str(case_dir / "runtime_console.log")
        result["artifact_unity_output_log"] = str(case_dir / "unity_output_log.txt")
        result["artifact_combined_log"] = str(case_dir / "combined_log.txt")
        if not passed:
            gh_error(f"{manifest_id}: {message}")
        else:
            gh_notice(f"{manifest_id}: {message}")
        return result
    except Exception as exc:
        result["passed"] = False
        result["message"] = str(exc)
        (case_dir / "failure.txt").write_text(f"{exc}\n", encoding="utf-8")
        gh_error(f"{manifest_id}: {exc}")
        return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--tools-root", required=True)
    parser.add_argument("--work-root", required=True)
    parser.add_argument("--artifacts-root", required=True)
    parser.add_argument("--patchpc-path", required=True)
    parser.add_argument("--run-seconds", type=int, default=120)
    parser.add_argument("--patch-path", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    tools_root = Path(args.tools_root).resolve()
    work_root = Path(args.work_root).resolve()
    artifacts_root = Path(args.artifacts_root).resolve()
    patchpc_path = (repo_root / args.patchpc_path).resolve()
    step_summary_env = os.environ.get("GITHUB_STEP_SUMMARY", "").strip()
    step_summary = Path(step_summary_env) if step_summary_env else None

    username = ensure_secret("STEAM_USERNAME")
    password = ensure_secret("STEAM_PASSWORD")
    twofactor = os.environ.get("STEAM_TWO_FACTOR_CODE", "").strip()

    if not patchpc_path.exists():
        raise WorkflowError(f"PATCHPC helper patch was not found: {patchpc_path}")

    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    if work_root.exists():
        shutil.rmtree(work_root)
    artifacts_root.mkdir(parents=True, exist_ok=True)
    work_root.mkdir(parents=True, exist_ok=True)

    depotdownloader_exe = ensure_depotdownloader(tools_root)
    patch_files = changed_patch_files(repo_root, args.patch_path.strip() or None)
    if not patch_files:
        gh_notice("No changed Patch.json files were found.")
        write_summary(step_summary, "# 2016 patch test\n\nNo changed `Patch.json` files were found.\n")
        return 0

    results: list[dict[str, Any]] = []
    for patch_file in patch_files:
        log("")
        log(f"=== Testing {patch_file.relative_to(repo_root).as_posix()} ===")
        results.append(
            test_one_patch(
                repo_root=repo_root,
                patch_path=patch_file,
                patchpc_path=patchpc_path,
                depotdownloader_exe=depotdownloader_exe,
                username=username,
                password=password,
                twofactor=twofactor,
                run_seconds=args.run_seconds,
                artifacts_root=artifacts_root,
                work_root=work_root,
            )
        )

    summary_lines = ["# 2016 patch test", ""]
    for result in results:
        status = "PASS" if result["passed"] else "FAIL"
        summary_lines.append(f"- **{result['manifest_id']}** — {status} — {result['message']}")
        summary_lines.append("  - Artifacts: `runtime_console.log`, `unity_output_log.txt`, `output_log.txt`, `combined_log.txt`")
    summary_lines.append("")

    summary_text = "\n".join(summary_lines) + "\n"
    (artifacts_root / "summary.md").write_text(summary_text, encoding="utf-8")
    save_json(artifacts_root / "results.json", results)
    write_summary(step_summary, summary_text)

    failed = [result for result in results if not result["passed"]]
    if failed:
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        gh_error(str(exc))
        raise
