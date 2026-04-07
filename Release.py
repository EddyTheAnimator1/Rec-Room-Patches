import base64
import ctypes
import getpass
import json
import os
import shutil
import subprocess
import traceback
import zipfile
import sys
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from urllib import error, request

APP_ID = 471710
DEPOT_ID = 471711
CONFIG_NAME = "steam_build_ui_release.json"
LOG_NAME = "last_depotdownloader_release.log"
TOOLS_DIR_NAME = "DepotDownloader"
DOWNLOAD_CHUNK = 1024 * 128
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
)
DEPOTDOWNLOADER_RELEASE_API = "https://api.github.com/repos/SteamRE/DepotDownloader/releases/latest"
PATCH_REPO_OWNER = "EddyTheAnimator1"
PATCH_REPO_NAME = "Rec-Room-Patches"
PATCH_BRANCHES = ("main", "master")
GITHUB_TREE_API = "https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
GITHUB_RAW_FILE_URL = "https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
RELEASE_DATA_DIR_NAME = "Recroom_Release_Data"
APP_VERSION = "0.0.1"
SELF_UPDATE_REPO_OWNER = "EddyTheAnimator1"
SELF_UPDATE_REPO_NAME = "Rec-Room-Patches"
GITHUB_LATEST_RELEASE_API = "https://api.github.com/repos/{owner}/{repo}/releases/latest"
GITHUB_RELEASES_PAGE_URL = "https://github.com/{owner}/{repo}/releases/latest"

INVALID_WIN_CHARS = {
    '<': '‹',
    '>': '›',
    ':': '.',
    '"': "'",
    '/': '∕',
    '\\': '∖',
    '|': 'ǀ',
    '?': '？',
    '*': '＊',
}

EXE_NAME_PREFERENCES = [
    "RecRoom_Release.exe",
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

_TREE_CACHE: dict[str, list[str]] = {}


class UI:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"

    @staticmethod
    def enable_ansi() -> None:
        if os.name == "nt":
            os.system("")

    @staticmethod
    def clear() -> None:
        os.system("cls" if os.name == "nt" else "clear")

    @staticmethod
    def line(char: str = "-", width: int = 64, color: str = DIM) -> None:
        print(f"{color}{char * width}{UI.RESET}")

    @staticmethod
    def title(text: str) -> None:
        UI.line("=", color=UI.MAGENTA)
        print(f"{UI.BOLD}{UI.CYAN}{text}{UI.RESET}")
        UI.line("=", color=UI.MAGENTA)

    @staticmethod
    def section(text: str) -> None:
        print(f"\n{UI.BOLD}{UI.MAGENTA}{text}{UI.RESET}")
        UI.line(color=UI.MAGENTA)

    @staticmethod
    def info(text: str) -> None:
        print(f"{UI.BLUE}[INFO]{UI.RESET} {text}")

    @staticmethod
    def ok(text: str) -> None:
        print(f"{UI.GREEN}[OK]{UI.RESET} {text}")

    @staticmethod
    def warn(text: str) -> None:
        print(f"{UI.YELLOW}[WARN]{UI.RESET} {text}")

    @staticmethod
    def err(text: str) -> None:
        print(f"{UI.RED}[ERR]{UI.RESET} {text}")

    @staticmethod
    def huge_warning(lines: list[str]) -> None:
        UI.line("!", color=UI.RED)
        print(f"{UI.BOLD}{UI.RED}WARNING . . WARNING . . WARNING{UI.RESET}")
        UI.line("!", color=UI.RED)
        for line in lines:
            print(f"{UI.BOLD}{UI.YELLOW}{line}{UI.RESET}")
        UI.line("!", color=UI.RED)


class DPAPIError(RuntimeError):
    pass


class DownloadError(RuntimeError):
    pass


class PatchError(RuntimeError):
    pass


class ShortcutError(RuntimeError):
    pass


class DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", ctypes.c_uint32),
        ("pbData", ctypes.POINTER(ctypes.c_ubyte)),
    ]


@dataclass
class PatchLookup:
    manifest_id: str
    found: bool
    folder_name: str | None = None
    raw_label: str | None = None
    safe_label: str | None = None
    branch: str | None = None
    patch_path: str | None = None
    patch_payload: dict | list | None = None
    warning: str | None = None


@dataclass
class PatchResult:
    file_path: Path
    summary: str


@dataclass
class LocalBuild:
    path: Path
    name: str
    exe_path: Path | None
    modified_ts: float



def _blob_from_bytes(data: bytes):
    keepalive = ctypes.create_string_buffer(data)
    blob = DATA_BLOB(len(data), ctypes.cast(keepalive, ctypes.POINTER(ctypes.c_ubyte)))
    return blob, keepalive



def _bytes_from_blob(blob: DATA_BLOB) -> bytes:
    if not blob.cbData:
        return b""
    return ctypes.string_at(blob.pbData, blob.cbData)



def dpapi_encrypt_to_b64(text: str) -> str:
    if os.name != "nt":
        raise DPAPIError("Windows DPAPI is required for stored credentials.")

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    data_in, keepalive = _blob_from_bytes(text.encode("utf-8"))
    data_out = DATA_BLOB()

    ok = crypt32.CryptProtectData(
        ctypes.byref(data_in),
        None,
        None,
        None,
        None,
        0x01,
        ctypes.byref(data_out),
    )
    if not ok:
        raise DPAPIError(f"CryptProtectData failed: {ctypes.GetLastError()}")

    try:
        return base64.b64encode(_bytes_from_blob(data_out)).decode("ascii")
    finally:
        if data_out.pbData:
            kernel32.LocalFree(data_out.pbData)
        del keepalive



def dpapi_decrypt_from_b64(value: str) -> str:
    if os.name != "nt":
        raise DPAPIError("Windows DPAPI is required for stored credentials.")

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    raw = base64.b64decode(value.encode("ascii"))
    data_in, keepalive = _blob_from_bytes(raw)
    data_out = DATA_BLOB()

    ok = crypt32.CryptUnprotectData(
        ctypes.byref(data_in),
        None,
        None,
        None,
        None,
        0x01,
        ctypes.byref(data_out),
    )
    if not ok:
        raise DPAPIError(f"CryptUnprotectData failed: {ctypes.GetLastError()}")

    try:
        return _bytes_from_blob(data_out).decode("utf-8")
    finally:
        if data_out.pbData:
            kernel32.LocalFree(data_out.pbData)
        del keepalive



def script_dir() -> Path:
    return Path(__file__).resolve().parent



def tools_dir() -> Path:
    return script_dir() / TOOLS_DIR_NAME



def config_path() -> Path:
    return script_dir() / CONFIG_NAME



def log_path() -> Path:
    return script_dir() / LOG_NAME



def depot_root() -> Path:
    return script_dir() / "depots" / str(DEPOT_ID)



def build_storage_root(config: dict | None = None) -> Path:
    if config is None:
        config = load_config()

    raw = config.get("build_storage_dir")
    if isinstance(raw, str) and raw.strip():
        return Path(os.path.expandvars(raw.strip().strip('"'))).expanduser().resolve(strict=False)

    return depot_root()



def desktop_dir() -> Path:
    home = Path(os.environ.get("USERPROFILE") or Path.home())
    return home / "Desktop"



def print_intro(build_count: int | None = None) -> None:
    UI.title("Rec Room Build Service [Release Build]")
    print(f"App ID        : {APP_ID}")
    print(f"Depot ID      : {DEPOT_ID}")
    print(f"Build storage : {build_storage_root()}")
    print("Patch mode    : GitHub manifest Patch.json")
    if build_count is not None:
        print(f"Local builds  : {build_count}")
    UI.line(color=UI.MAGENTA)



def load_config() -> dict:
    path = config_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        UI.warn("Config exists but could not be read. Starting with a new one.")
        return {}



def save_config(config: dict) -> None:
    config_path().write_text(json.dumps(config, indent=2), encoding="utf-8")



def pause_close() -> None:
    try:
        input("\nPress Enter to close . . .")
    except EOFError:
        pass



def press_enter(message: str = "Press Enter to continue . . .") -> None:
    try:
        input(f"\n{message}")
    except EOFError:
        pass



def prompt_nonempty(label: str, default: str | None = None) -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default is not None:
            return default
        UI.warn("This field cannot be empty.")



def prompt_yes_no(label: str, default: bool = True) -> bool:
    suffix = "Y/n" if default else "y/N"
    while True:
        raw = input(f"{label} [{suffix}]: ").strip().lower()
        if not raw:
            return default
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        UI.warn("Enter yes or no.")



def prompt_menu_choice(valid: set[str], label: str = "Select option") -> str:
    while True:
        value = input(f"{label}: ").strip().lower()
        if value in valid:
            return value
        UI.warn("That option is not valid.")



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



def make_windows_safe(name: str) -> str:
    cleaned = []
    for ch in name.strip():
        cleaned.append(INVALID_WIN_CHARS.get(ch, ch))
    value = "".join(cleaned).strip(" .")
    return value or "UnknownBuild"



def make_unique_path(path: Path, force_full_name: bool = False) -> Path:
    if not path.exists():
        return path
    counter = 2
    while True:
        if force_full_name:
            candidate_name = f"{path.name} ({counter})"
        else:
            candidate_name = f"{path.stem} ({counter}){path.suffix}"
        candidate = path.with_name(candidate_name)
        if not candidate.exists():
            return candidate
        counter += 1



def request_bytes(url: str, extra_headers: dict[str, str] | None = None) -> bytes:
    headers = {"User-Agent": USER_AGENT}
    if extra_headers:
        headers.update(extra_headers)
    req = request.Request(url, headers=headers)
    with request.urlopen(req, timeout=30) as resp:
        return resp.read()



def request_text(url: str, extra_headers: dict[str, str] | None = None) -> str:
    return request_bytes(url, extra_headers).decode("utf-8", errors="replace")



def request_json(url: str) -> dict:
    return json.loads(
        request_text(
            url,
            {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
    )


def normalize_version_tag(value: str) -> str:
    return value.strip().lstrip("vV")


def parse_version_parts(value: str) -> tuple[int | str, ...]:
    normalized = normalize_version_tag(value)
    parts: list[int | str] = []
    for chunk in normalized.replace("-", ".").split("."):
        item = chunk.strip()
        if not item:
            continue
        parts.append(int(item) if item.isdigit() else item.lower())
    return tuple(parts)


def is_probably_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def current_executable_path() -> Path:
    return Path(sys.executable if is_probably_frozen() else __file__).resolve()


def latest_release_api_url() -> str:
    return GITHUB_LATEST_RELEASE_API.format(
        owner=SELF_UPDATE_REPO_OWNER,
        repo=SELF_UPDATE_REPO_NAME,
    )


def latest_release_page_url() -> str:
    return GITHUB_RELEASES_PAGE_URL.format(
        owner=SELF_UPDATE_REPO_OWNER,
        repo=SELF_UPDATE_REPO_NAME,
    )


def fetch_latest_release_version() -> str | None:
    payload = request_json(latest_release_api_url())
    tag_name = payload.get("tag_name")
    if isinstance(tag_name, str) and tag_name.strip():
        return normalize_version_tag(tag_name)
    return None


def is_outdated_version(current_version: str, latest_version: str) -> bool:
    current_parts = parse_version_parts(current_version)
    latest_parts = parse_version_parts(latest_version)
    return current_parts < latest_parts


def enforce_latest_release() -> int:
    if not is_probably_frozen():
        return 0

    current_version = normalize_version_tag(APP_VERSION)
    if not current_version or current_version.endswith("-dev"):
        UI.warn("Release version is not embedded in this build. Update lock check skipped.")
        return 0

    try:
        latest_version = fetch_latest_release_version()
    except Exception as exc:
        UI.warn(f"Could not check for updates right now: {exc}")
        return 0

    if not latest_version:
        UI.warn("Could not determine the latest GitHub release version.")
        return 0

    if not is_outdated_version(current_version, latest_version):
        return 0

    UI.huge_warning(
        [
            "This Release.exe is outdated and will now close.",
            f"Current version : v{current_version}",
            f"Latest version  : v{latest_version}",
            "The GitHub releases page will open so you can download the latest version.",
        ]
    )

    try:
        webbrowser.open(latest_release_page_url())
        UI.ok("Opened GitHub releases page.")
    except Exception as exc:
        UI.warn(f"Could not open the browser automatically: {exc}")
        UI.info(f"Open this page manually: {latest_release_page_url()}")

    return 2



def choose_release_asset(payload: dict) -> tuple[str, str]:
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
        raise DownloadError("Could not find a Windows DepotDownloader release asset.")
    _, name, url = scored[0]
    return name, url



def find_depotdownloader_command() -> list[str] | None:
    search_roots = [script_dir(), tools_dir()]
    exes: list[Path] = []
    dlls: list[Path] = []

    for root in search_roots:
        if not root.exists():
            continue
        exes.extend(root.rglob("DepotDownloader.exe"))
        dlls.extend(root.rglob("DepotDownloader.dll"))

    if exes:
        exes.sort(key=lambda p: len(str(p)))
        return [str(exes[0])]
    if dlls:
        dlls.sort(key=lambda p: len(str(p)))
        return ["dotnet", str(dlls[0])]
    return None



def download_file(url: str, dest: Path) -> None:
    req = request.Request(url, headers={"User-Agent": USER_AGENT})
    with request.urlopen(req, timeout=60) as resp:
        total_header = resp.headers.get("Content-Length")
        total = int(total_header) if total_header and total_header.isdigit() else 0
        read = 0
        with dest.open("wb") as f:
            while True:
                chunk = resp.read(DOWNLOAD_CHUNK)
                if not chunk:
                    break
                f.write(chunk)
                read += len(chunk)
                if total > 0:
                    percent = (read / total) * 100
                    print(f"\r{UI.BLUE}[DL]{UI.RESET} {dest.name} {percent:6.2f}%", end="", flush=True)
                else:
                    print(f"\r{UI.BLUE}[DL]{UI.RESET} {dest.name} {read // 1024} KB", end="", flush=True)
    print()



def ensure_depotdownloader() -> list[str]:
    existing = find_depotdownloader_command()
    if existing is not None:
        UI.ok("DepotDownloader already exists.")
        return existing

    UI.section("DepotDownloader Setup")
    UI.info("DepotDownloader not found. Downloading it now...")
    tools_dir().mkdir(parents=True, exist_ok=True)

    try:
        release = request_json(DEPOTDOWNLOADER_RELEASE_API)
        asset_name, asset_url = choose_release_asset(release)
    except error.HTTPError as exc:
        raise DownloadError(f"Failed to query GitHub release info: HTTP {exc.code}") from exc
    except error.URLError as exc:
        raise DownloadError(f"Could not reach GitHub: {exc}") from exc

    zip_path = tools_dir() / asset_name
    extract_dir = tools_dir() / "current"
    temp_extract_dir = tools_dir() / "_extracting"

    if temp_extract_dir.exists():
        shutil.rmtree(temp_extract_dir, ignore_errors=True)

    download_file(asset_url, zip_path)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(temp_extract_dir)
    except zipfile.BadZipFile as exc:
        raise DownloadError("Downloaded DepotDownloader archive is invalid.") from exc

    if extract_dir.exists():
        shutil.rmtree(extract_dir, ignore_errors=True)
    temp_extract_dir.rename(extract_dir)

    found = find_depotdownloader_command()
    if found is None:
        raise DownloadError("DepotDownloader was downloaded, but the executable could not be found after extraction.")

    UI.ok(f"DepotDownloader downloaded into: {extract_dir}")
    UI.clear()
    print_intro(count_local_builds())
    return found



def ensure_credentials(config: dict) -> tuple[str, str]:
    stored_user = config.get("steam_username")
    stored_pass = config.get("steam_password_b64")

    if stored_user and stored_pass:
        try:
            password = dpapi_decrypt_from_b64(stored_pass)
            UI.ok(f"Loaded Steam credentials for {stored_user}.")
            if prompt_yes_no("Reuse saved Steam credentials", True):
                return stored_user, password
        except Exception:
            UI.warn("Saved credentials could not be decrypted. They will be replaced.")

    UI.section("Steam Login Setup")
    UI.info("Password is protected with Windows DPAPI and stored in the local JSON file.")
    username = prompt_nonempty("Steam username")
    password = getpass.getpass("Steam password: ").strip()
    if not password:
        raise ValueError("Steam password cannot be empty.")

    config["steam_username"] = username
    config["steam_password_b64"] = dpapi_encrypt_to_b64(password)
    save_config(config)
    UI.ok("Steam credentials saved.")
    return username, password



def clear_saved_credentials(config: dict) -> None:
    config.pop("steam_username", None)
    config.pop("steam_password_b64", None)
    save_config(config)



def prompt_manifest() -> str:
    UI.section("Manifest Input")
    UI.info("Enter the manifest ID you want to download.")
    while True:
        value = prompt_nonempty("Manifest ID")
        if value.isdigit() and len(value) >= 5:
            return value
        UI.warn("Manifest ID must be numeric.")


def parse_optional_patch_label(payload: dict | list | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("label", "date", "name", "title", "build_label"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def open_patch_file_dialog() -> Path | None:
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        root.update()
        selected = filedialog.askopenfilename(
            title="Select Patch.json",
            initialdir=str(script_dir()),
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        root.destroy()
    except Exception as exc:
        UI.warn(f"Could not open file picker: {exc}")
        return None

    if not selected:
        return None
    return Path(selected).resolve(strict=False)



def load_github_patch_payload(bundle: PatchLookup) -> tuple[str | None, dict | list | None]:
    UI.section("GitHub Patch Lookup")
    if not bundle.found:
        UI.warn("Manifest folder was not found in the GitHub repo. Download will continue without patch instructions.")
        return None, None

    if bundle.patch_path is None:
        UI.warn("Manifest folder exists, but Patch.json was not found there. Download will continue without patch instructions.")
        return None, None

    if bundle.patch_payload is None:
        UI.warn("Patch.json was found on GitHub, but it could not be loaded. Download will continue without patch instructions.")
        return bundle.patch_path, None

    UI.ok(f"GitHub patch loaded: {bundle.patch_path}")
    return bundle.patch_path, bundle.patch_payload



def get_repo_tree(branch: str) -> list[str]:
    cache_key = f"{PATCH_REPO_OWNER}/{PATCH_REPO_NAME}:{branch}"
    if cache_key in _TREE_CACHE:
        return _TREE_CACHE[cache_key]

    url = GITHUB_TREE_API.format(owner=PATCH_REPO_OWNER, repo=PATCH_REPO_NAME, branch=branch)
    payload = request_json(url)
    paths = [str(item.get("path", "")) for item in payload.get("tree", []) if item.get("path")]
    _TREE_CACHE[cache_key] = paths
    return paths



def choose_manifest_folder(manifest_id: str, folders: list[str]) -> str:
    exact = [folder for folder in folders if folder == manifest_id]
    if exact:
        return exact[0]

    startswith = [folder for folder in folders if folder.startswith(manifest_id + " ")]
    if startswith:
        startswith.sort(key=len)
        return startswith[0]

    folders.sort(key=lambda item: (len(item), item.lower()))
    return folders[0]



def parse_date_json_text(text: str) -> str:
    value = json.loads(text)
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, dict):
        for key in ("date", "label", "name", "title"):
            current = value.get(key)
            if isinstance(current, str) and current.strip():
                return current.strip()
    raise PatchError("Date.json exists, but it does not contain a usable date string.")



def fetch_repo_file(branch: str, path: str) -> str:
    url = GITHUB_RAW_FILE_URL.format(owner=PATCH_REPO_OWNER, repo=PATCH_REPO_NAME, branch=branch, path=path)
    return request_text(url)



def lookup_manifest_bundle(manifest_id: str) -> PatchLookup:
    for branch in PATCH_BRANCHES:
        try:
            paths = get_repo_tree(branch)
        except Exception:
            continue

        date_paths = [
            path for path in paths
            if path.startswith("manifest/") and path.endswith("/Date.json") and manifest_id in Path(path).parent.name
        ]
        if not date_paths:
            continue

        folders = [Path(path).parent.name for path in date_paths]
        folder_name = choose_manifest_folder(manifest_id, folders)
        date_path = f"manifest/{folder_name}/Date.json"

        try:
            raw_label = parse_date_json_text(fetch_repo_file(branch, date_path))
        except Exception as exc:
            return PatchLookup(
                manifest_id=manifest_id,
                found=True,
                folder_name=folder_name,
                branch=branch,
                warning=f"Found manifest folder, but Date.json could not be read: {exc}",
            )

        patch_path = None
        for candidate in (
            f"manifest/{folder_name}/Patch.json",
            f"manifest/{folder_name}/patch.json",
        ):
            if candidate in paths:
                patch_path = candidate
                break

        patch_payload: dict | list | None = None
        if patch_path is not None:
            try:
                patch_payload = json.loads(fetch_repo_file(branch, patch_path))
            except Exception as exc:
                return PatchLookup(
                    manifest_id=manifest_id,
                    found=True,
                    folder_name=folder_name,
                    raw_label=raw_label,
                    safe_label=make_windows_safe(raw_label),
                    branch=branch,
                    patch_path=patch_path,
                    warning=f"Patch.json exists, but it could not be parsed: {exc}",
                )

        return PatchLookup(
            manifest_id=manifest_id,
            found=True,
            folder_name=folder_name,
            raw_label=raw_label,
            safe_label=make_windows_safe(raw_label),
            branch=branch,
            patch_path=patch_path,
            patch_payload=patch_payload,
        )

    return PatchLookup(
        manifest_id=manifest_id,
        found=False,
        safe_label=manifest_id,
        warning=(
            f"Manifest not indexed in {PATCH_REPO_NAME}: could not find any manifest folder containing {manifest_id}."
        ),
    )



def save_process_log(lines: list[str]) -> None:
    log_path().write_text("\n".join(lines), encoding="utf-8", errors="replace")



def stream_process(args: list[str]) -> tuple[int, list[str]]:
    UI.section("Running DepotDownloader")
    UI.info(mask_command(args))
    process = subprocess.Popen(
        args,
        cwd=str(script_dir()),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=None,
        text=True,
        bufsize=1,
    )
    captured: list[str] = []
    assert process.stdout is not None
    for raw_line in process.stdout:
        line = raw_line.rstrip("\r\n")
        captured.append(line)
        print(line)
    return process.wait(), captured



def snapshot_depot_dirs() -> set[str]:
    root = depot_root()
    if not root.exists():
        return set()
    return {child.name for child in root.iterdir() if child.is_dir()}



def locate_downloaded_folder(before_dirs: set[str]) -> Path:
    root = depot_root()
    if not root.exists():
        raise FileNotFoundError(f"Expected depot directory was not created: {root}")

    current_dirs = [child for child in root.iterdir() if child.is_dir()]
    new_dirs = [child for child in current_dirs if child.name not in before_dirs]

    if len(new_dirs) == 1:
        return new_dirs[0]
    if len(current_dirs) == 1:
        return current_dirs[0]

    raise FileNotFoundError(
        "DepotDownloader finished, but the script could not uniquely determine the inner depot folder to rename."
    )



def finalize_downloaded_folder(
    before_dirs: set[str],
    label: str,
    destination_root: Path,
    on_existing: str = "replace",
) -> Path:
    source = locate_downloaded_folder(before_dirs)
    destination_root.mkdir(parents=True, exist_ok=True)
    target = destination_root / make_windows_safe(label)

    try:
        same_target = source.resolve() == target.resolve()
    except FileNotFoundError:
        same_target = False

    if same_target:
        return source

    if target.exists():
        if on_existing == "replace":
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        elif on_existing == "keep_both":
            target = make_unique_path(target, force_full_name=True)
        else:
            raise RuntimeError(f"Unknown existing-build action: {on_existing}")

    moved_path = Path(shutil.move(str(source), str(target)))
    return moved_path



def normalize_patch_instructions(payload: dict | list) -> list[dict]:
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

    normalized: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            raise PatchError("Every patch instruction must be a JSON object.")

        kind = str(item.get("type") or item.get("kind") or "replace_bytes").strip().lower()
        file_value = item.get("file") or item.get("path")
        if not isinstance(file_value, str) or not file_value.strip():
            raise PatchError("Each patch instruction needs a file/path string.")

        if kind in {"write_text_file", "create_text_file", "write_text"}:
            content = item.get("content")
            if content is None:
                content = item.get("text", item.get("value"))
            if not isinstance(content, str):
                raise PatchError("Text-file patch instructions need a string content/text/value field.")

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



def release_data_dir(build_dir: Path) -> Path:
    data_dir = build_dir / RELEASE_DATA_DIR_NAME
    if not data_dir.exists() or not data_dir.is_dir():
        raise PatchError(f"{RELEASE_DATA_DIR_NAME} was not found in the build folder: {build_dir}")
    return data_dir



def resolve_target_file(base_dir: Path, file_value: str) -> Path:
    relative_path = Path(file_value)
    direct = base_dir / relative_path
    if direct.exists() and direct.is_file():
        return direct

    name = relative_path.name
    matches = [path for path in base_dir.rglob(name) if path.is_file()]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise PatchError(f"Patch target file was not found: {file_value}")
    raise PatchError(f"Patch target file is ambiguous: {file_value}")



def resolve_patch_target_file(build_dir: Path, file_value: str, kind: str) -> Path:
    relative_path = Path(file_value)
    if kind in {"write_text_file", "create_text_file", "write_text"}:
        return build_dir / relative_path

    base_dir = release_data_dir(build_dir)
    parts_lower = [part.lower() for part in relative_path.parts]
    if RELEASE_DATA_DIR_NAME.lower() in parts_lower:
        return resolve_target_file(build_dir, file_value)
    return resolve_target_file(base_dir, file_value)



def ensure_backup(path: Path) -> None:
    backup = path.with_name(path.name + ".bak")
    if not backup.exists():
        shutil.copy2(path, backup)



def apply_replace_bytes(file_path: Path, replacements: list[dict], encoding: str) -> int:
    data = file_path.read_bytes()
    total_applied = 0

    for index, replacement in enumerate(replacements, start=1):
        if not isinstance(replacement, dict):
            raise PatchError(f"Replacement #{index} is not a JSON object.")

        find_value = replacement.get("find")
        replace_value = replacement.get("replace")
        if not isinstance(find_value, str) or not isinstance(replace_value, str):
            raise PatchError(f"Replacement #{index} needs string find and replace values.")

        find_bytes = find_value.encode(encoding)
        replace_bytes = replace_value.encode(encoding)
        if len(find_bytes) != len(replace_bytes):
            raise PatchError(
                f"Replacement #{index} changes byte length for {file_path.name}. That is blocked by default."
            )

        count = data.count(find_bytes)
        if count <= 0:
            raise PatchError(f"Replacement #{index} could not find its target inside {file_path.name}.")

        data = data.replace(find_bytes, replace_bytes)
        total_applied += count
        UI.ok(f"{file_path.name}: replacement #{index} applied {count} time(s)")

    ensure_backup(file_path)
    file_path.write_bytes(data)
    return total_applied



def write_text_file(target_path: Path, content: str, encoding: str, overwrite: bool) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and target_path.is_dir():
        raise PatchError(f"Patch target path is a directory, not a file: {target_path}")
    if target_path.exists():
        if not overwrite:
            raise PatchError(f"Patch target file already exists and overwrite is disabled: {target_path.name}")
        ensure_backup(target_path)

    with target_path.open("w", encoding=encoding, newline="") as handle:
        handle.write(content)



def apply_patch_payload(build_dir: Path, payload: dict | list) -> list[PatchResult]:
    instructions = normalize_patch_instructions(payload)
    results: list[PatchResult] = []

    UI.section("Applying Patch Instructions")
    UI.info(f"write_text_file target root : {build_dir}")
    UI.info(f"replace_bytes target root   : {release_data_dir(build_dir)}")
    for instruction in instructions:
        kind = instruction["type"]
        target_file = resolve_patch_target_file(build_dir, instruction["file"], kind)

        if kind in {"write_text_file", "create_text_file", "write_text"}:
            write_text_file(
                target_file,
                instruction["content"],
                instruction["encoding"],
                instruction["overwrite"],
            )
            UI.ok(f"{target_file.relative_to(build_dir)}: text file written")
            results.append(PatchResult(file_path=target_file, summary="text file written"))
            continue

        if kind not in {"replace_bytes", "replace_text", "replace_strings"}:
            raise PatchError(f"Unsupported patch type: {kind}")

        replacements_applied = apply_replace_bytes(
            target_file,
            instruction["replacements"],
            instruction["encoding"],
        )
        results.append(PatchResult(file_path=target_file, summary=f"{replacements_applied} replacement(s) applied"))

    return results



def open_path(path: Path) -> None:
    if os.name != "nt":
        raise RuntimeError("Opening paths is only wired for Windows here.")
    os.startfile(str(path))



def score_executable(path: Path, build_dir: Path) -> tuple[int, int, str]:
    name = path.name.lower()
    score = 0
    for index, preferred in enumerate(EXE_NAME_PREFERENCES):
        if path.name.lower() == preferred.lower():
            score += 200 - index * 10
    if path.parent == build_dir:
        score += 40
    if "recroom" in name:
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



def find_launch_executable(build_dir: Path) -> Path | None:
    if not build_dir.exists():
        return None
    candidates = [path for path in build_dir.rglob("*.exe") if path.is_file()]
    filtered = [path for path in candidates if path.name not in EXE_NAME_BLOCKLIST]
    if not filtered:
        filtered = candidates
    if not filtered:
        return None
    filtered.sort(key=lambda item: score_executable(item, build_dir), reverse=True)
    best = filtered[0]
    if score_executable(best, build_dir)[0] < 0:
        return None
    return best



def count_local_builds() -> int:
    return len(scan_local_builds())



def scan_local_builds() -> list[LocalBuild]:
    root = build_storage_root()
    if not root.exists():
        return []

    builds: list[LocalBuild] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            modified_ts = child.stat().st_mtime
        except OSError:
            modified_ts = 0.0
        builds.append(
            LocalBuild(
                path=child,
                name=child.name,
                exe_path=find_launch_executable(child),
                modified_ts=modified_ts,
            )
        )

    builds.sort(key=lambda item: (-item.modified_ts, item.name.lower()))
    return builds



def print_local_builds(builds: list[LocalBuild]) -> None:
    UI.section("Local Builds")
    if not builds:
        UI.warn("No local builds were found yet.")
        return

    for index, build in enumerate(builds, start=1):
        exe_name = build.exe_path.name if build.exe_path else "no launcher found"
        print(f"{index:>2}. {build.name}")
        print(f"    path : {build.path}")
        print(f"    run  : {exe_name}")
        UI.line(width=48)



def choose_local_build(builds: list[LocalBuild], label: str = "Build number") -> LocalBuild | None:
    if not builds:
        return None
    while True:
        raw = input(f"{label} (or B to go back): ").strip().lower()
        if raw in {"b", "back", "0"}:
            return None
        if raw.isdigit():
            index = int(raw)
            if 1 <= index <= len(builds):
                return builds[index - 1]
        UI.warn("That build number is not valid.")



def powershell_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"



def create_windows_shortcut(shortcut_path: Path, target_path: Path, working_dir: Path, description: str, icon_path: Path | None = None) -> Path:
    shortcut_path = make_unique_path(shortcut_path)
    parts = [
        "$W = New-Object -ComObject WScript.Shell",
        f"$S = $W.CreateShortcut({powershell_quote(str(shortcut_path))})",
        f"$S.TargetPath = {powershell_quote(str(target_path))}",
        f"$S.WorkingDirectory = {powershell_quote(str(working_dir))}",
        f"$S.Description = {powershell_quote(description)}",
    ]
    if icon_path is not None:
        parts.append(f"$S.IconLocation = {powershell_quote(str(icon_path))}")
    parts.append("$S.Save()")
    script = "; ".join(parts)

    try:
        subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                script,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or exc.stdout or "").strip()
        raise ShortcutError(stderr or "PowerShell could not create the shortcut.") from exc

    return shortcut_path



def create_build_shortcut(build: LocalBuild) -> Path:
    desktop = desktop_dir()
    desktop.mkdir(parents=True, exist_ok=True)
    target = build.exe_path or build.path
    working_dir = build.exe_path.parent if build.exe_path else build.path
    icon_path = build.exe_path if build.exe_path else None
    shortcut_name = make_windows_safe(build.name) + ".lnk"
    shortcut_path = desktop / shortcut_name
    description = f"Rec Room build: {build.name}"
    return create_windows_shortcut(shortcut_path, target, working_dir, description, icon_path)



def launch_build(build: LocalBuild) -> None:
    target = build.exe_path or build.path
    open_path(target)



def handle_build_actions(build: LocalBuild) -> None:
    while True:
        UI.clear()
        print_intro(count_local_builds())
        UI.section(f"Build: {build.name}")
        print(f"Path   : {build.path}")
        print(f"Launch : {build.exe_path if build.exe_path else 'No executable found'}")
        UI.line(color=UI.MAGENTA)
        print("1. Open build folder")
        print("2. Launch build")
        print("3. Create desktop shortcut")
        print("0. Back")
        UI.line(color=UI.MAGENTA)

        choice = prompt_menu_choice({"1", "2", "3", "0"})
        if choice == "0":
            return
        if choice == "1":
            open_path(build.path)
            UI.ok("Build folder opened.")
            press_enter()
        elif choice == "2":
            if build.exe_path is None:
                UI.warn("No launchable .exe was found for this build.")
            else:
                launch_build(build)
                UI.ok(f"Launched: {build.exe_path.name}")
            press_enter()
        elif choice == "3":
            shortcut = create_build_shortcut(build)
            UI.ok(f"Desktop shortcut created: {shortcut}")
            press_enter()



def browse_local_builds() -> None:
    while True:
        UI.clear()
        builds = scan_local_builds()
        print_intro(len(builds))
        print_local_builds(builds)
        if not builds:
            press_enter()
            return
        build = choose_local_build(builds)
        if build is None:
            return
        handle_build_actions(build)



def manage_credentials_menu(config: dict) -> None:
    while True:
        UI.clear()
        print_intro(count_local_builds())
        UI.section("Steam Login Settings")
        stored_user = config.get("steam_username")
        if stored_user:
            UI.ok(f"Saved username: {stored_user}")
        else:
            UI.warn("No Steam credentials are saved yet.")
        UI.line(color=UI.MAGENTA)
        print("1. Save / replace credentials")
        print("2. Remove saved credentials")
        print("0. Back")
        UI.line(color=UI.MAGENTA)

        choice = prompt_menu_choice({"1", "2", "0"})
        if choice == "0":
            return
        if choice == "1":
            ensure_credentials(config)
            press_enter()
        elif choice == "2":
            if not stored_user:
                UI.warn("There are no saved credentials to remove.")
            else:
                clear_saved_credentials(config)
                UI.ok("Saved Steam credentials removed.")
            press_enter()



def download_build_workflow(config: dict) -> None:
    UI.clear()
    print_intro(count_local_builds())
    depot_cmd = ensure_depotdownloader()
    steam_user, steam_password = ensure_credentials(config)
    manifest_id = prompt_manifest()

    UI.section("Build Label Lookup")
    UI.info(f"Checking {PATCH_REPO_NAME} for the manifest date label and Patch.json . . .")
    bundle = lookup_manifest_bundle(manifest_id)

    if bundle.warning:
        UI.huge_warning([bundle.warning])
    elif bundle.found and bundle.raw_label:
        UI.ok(f"GitHub date label found: {bundle.raw_label}")

    patch_file_path, patch_payload = load_github_patch_payload(bundle)

    raw_label = bundle.raw_label or parse_optional_patch_label(patch_payload) or manifest_id
    final_label = make_windows_safe(raw_label)

    UI.section("Build Label")
    UI.info(f"Final folder label: {raw_label}")

    if patch_file_path is not None and patch_payload is not None:
        UI.ok(f"Patch file in use: {patch_file_path}")
    elif patch_file_path is not None:
        UI.warn(f"Patch file was found but is not usable: {patch_file_path}")
    else:
        UI.warn("No patch instructions will be applied.")

    storage_root = build_storage_root(config)
    existing_build_path = storage_root / make_windows_safe(final_label)
    on_existing = "replace"
    if existing_build_path.exists():
        UI.huge_warning([
            "This build already exists locally.",
            f"Existing folder: {existing_build_path}",
            "R = replace existing build",
            "K = keep both copies",
            "C = cancel download",
        ])
        on_existing_choice = prompt_menu_choice({"r", "k", "c"}, "Choose R, K, or C")
        if on_existing_choice == "c":
            UI.info("Download cancelled. Returning to main menu . . .")
            return
        if on_existing_choice == "k":
            on_existing = "keep_both"

    before_dirs = snapshot_depot_dirs()
    cmd = [
        *depot_cmd,
        "-app", str(APP_ID),
        "-depot", str(DEPOT_ID),
        "-manifest", manifest_id,
        "-username", steam_user,
        "-password", steam_password,
    ]

    exit_code, output = stream_process(cmd)
    save_process_log(output)
    if exit_code != 0:
        UI.err(f"DepotDownloader exited with code {exit_code}")
        UI.warn(f"Full output saved to: {log_path()}")
        press_enter()
        return

    renamed_dir = finalize_downloaded_folder(before_dirs, final_label, storage_root, on_existing=on_existing)
    UI.section("Download Complete")
    UI.ok(f"Final folder: {renamed_dir}")

    if patch_payload is not None:
        try:
            results = apply_patch_payload(renamed_dir, patch_payload)
        except Exception as exc:
            UI.huge_warning([
                "GitHub Patch.json was loaded, but patching failed.",
                str(exc),
                f"Build folder: {renamed_dir}",
            ])
            press_enter()
            return

        UI.section("Patch Complete")
        for result in results:
            try:
                relative_name = result.file_path.relative_to(renamed_dir)
            except ValueError:
                relative_name = result.file_path
            UI.ok(f"{relative_name}: {result.summary}")
    else:
        UI.warn("No patch instructions were applied.")

    build = LocalBuild(
        path=renamed_dir,
        name=renamed_dir.name,
        exe_path=find_launch_executable(renamed_dir),
        modified_ts=renamed_dir.stat().st_mtime if renamed_dir.exists() else 0.0,
    )

    if prompt_yes_no("Create a desktop shortcut for this build", True):
        shortcut = create_build_shortcut(build)
        UI.ok(f"Desktop shortcut created: {shortcut}")

    if prompt_yes_no("Open this build now", False):
        open_path(build.path)
        UI.ok(f"Opened: {build.path}")

    UI.info("Returning to main menu . . .")


def create_shortcut_from_menu() -> None:
    UI.clear()
    builds = scan_local_builds()
    print_intro(len(builds))
    print_local_builds(builds)
    if not builds:
        press_enter()
        return
    build = choose_local_build(builds, "Build number for desktop shortcut")
    if build is None:
        return
    shortcut = create_build_shortcut(build)
    UI.ok(f"Desktop shortcut created: {shortcut}")
    press_enter()



def open_build_storage() -> None:
    root = build_storage_root()
    root.mkdir(parents=True, exist_ok=True)
    open_path(root)
    UI.ok(f"Opened build storage: {root}")
    press_enter()



def build_storage_settings_menu(config: dict) -> None:
    while True:
        UI.clear()
        current_root = build_storage_root(config)
        print_intro(count_local_builds())
        UI.section("Build Storage Settings")
        print(f"Current storage : {current_root}")
        print(f"Default storage : {depot_root()}")
        UI.line(color=UI.MAGENTA)
        print("1. Change build storage folder")
        print("2. Reset to default storage")
        print("3. Open current storage folder")
        print("0. Back")
        UI.line(color=UI.MAGENTA)

        choice = prompt_menu_choice({"1", "2", "3", "0"})
        if choice == "0":
            return
        if choice == "1":
            raw_path = prompt_nonempty("New build storage folder", str(current_root))
            target = Path(os.path.expandvars(raw_path.strip().strip('"'))).expanduser().resolve(strict=False)
            target.mkdir(parents=True, exist_ok=True)
            config["build_storage_dir"] = str(target)
            save_config(config)
            UI.ok(f"Build storage changed to: {target}")
            press_enter()
        elif choice == "2":
            config.pop("build_storage_dir", None)
            save_config(config)
            UI.ok(f"Build storage reset to default: {depot_root()}")
            press_enter()
        elif choice == "3":
            current_root.mkdir(parents=True, exist_ok=True)
            open_path(current_root)
            UI.ok(f"Opened build storage: {current_root}")
            press_enter()



def main() -> int:
    UI.enable_ansi()
    if os.name != "nt":
        UI.err("This script is Windows-only because credential storage and shortcut creation use Windows features.")
        return 1

    update_exit_code = enforce_latest_release()
    if update_exit_code != 0:
        return update_exit_code

    config = load_config()

    while True:
        UI.clear()
        print_intro(count_local_builds())
        UI.section("Welcome")
        print("1. Download and patch a build")
        print("2. Browse local builds")
        print("3. Create desktop shortcut")
        print("4. Open build storage folder")
        print("5. Steam login settings")
        print("6. Build storage settings")
        print("0. Exit")
        UI.line(color=UI.MAGENTA)

        choice = prompt_menu_choice({"1", "2", "3", "4", "5", "6", "0"})
        if choice == "0":
            return 0
        if choice == "1":
            download_build_workflow(config)
        elif choice == "2":
            browse_local_builds()
        elif choice == "3":
            create_shortcut_from_menu()
        elif choice == "4":
            open_build_storage()
        elif choice == "5":
            manage_credentials_menu(config)
        elif choice == "6":
            build_storage_settings_menu(config)


if __name__ == "__main__":
    code = 1
    try:
        code = main()
    except KeyboardInterrupt:
        print()
        UI.warn("Cancelled by user.")
        code = 130
    except Exception as exc:
        UI.err(str(exc))
        UI.warn("Detailed traceback:")
        traceback.print_exc()
        code = 1
    finally:
        pause_close()
    raise SystemExit(code)
