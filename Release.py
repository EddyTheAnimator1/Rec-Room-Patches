import base64
import ctypes
import getpass
import json
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import webbrowser
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib import error, parse, request


APP_ID = 471710
DEPOT_ID = 471711
APP_VERSION = "0.0.0-dev"
REMEMBERING_NAME = "Remembering.json"
LOG_NAME = "last_depotdownloader_noir.log"
ERROR_LOG_NAME = "last_release_noir_error.log"
PREVIEW_STATE_VERSION = 1
USER_AGENT = "Release-Noir/1.0"
DEPOTDOWNLOADER_RELEASE_API = "https://api.github.com/repos/SteamRE/DepotDownloader/releases/latest"
MELONLOADER_RELEASE_TAG = "v0.5.7"
MELONLOADER_ASSET_NAME = "MelonLoader.x64.zip"
MELONLOADER_RELEASE_API = "https://api.github.com/repos/LavaGang/MelonLoader/releases/tags/{tag}"
MELONLOADER_RELEASE_URL = "https://github.com/LavaGang/MelonLoader/releases/download/{tag}/{asset}"
MELONLOADER_PROMPT_INFO = (
    "Windows 11 has a 50/50 chance of crashing these builds. "
    "MelonLoader prevents that, don't ask why. Plus, it doesn't hurt to have it."
)
PATCH_REPO_OWNER = "EddyTheAnimator1"
PATCH_REPO_NAME = "Rec-Room-Patches"
PATCH_BRANCHES = ("main", "master")
SELF_UPDATE_REPO_OWNER = PATCH_REPO_OWNER
SELF_UPDATE_REPO_NAME = PATCH_REPO_NAME
GITHUB_LATEST_RELEASE_API = "https://api.github.com/repos/{owner}/{repo}/releases/latest"
GITHUB_RELEASES_PAGE_URL = "https://github.com/{owner}/{repo}/releases/latest"
GITHUB_TREE_API = "https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
GITHUB_RAW_FILE_URL = "https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
DOWNLOAD_CHUNK = 1024 * 256
STEP_DELAY = 0.01
BETA_MANIFESTS = {
    "2932388464690083659": "dankr",
    "8267987913704360820": "tmoney_trailer",
    "6185049689623293718": "pedro_test",
    "4693569285935572384": "index_improved",
}

PERCENT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")
LEADING_PERCENT_RE = re.compile(r"^\s*\d+(?:\.\d+)?\s*%\s*")
STEAM_GUARD_PROMPT_RE = re.compile(
    r"(?:STEAM GUARD!\s+)?Please enter (?:"
    r"the auth code sent to the email at [^:\r\n]+|"
    r"the authentication code sent to your email address|"
    r"your 2 factor auth code from your authenticator app"
    r"):\s*",
    re.IGNORECASE,
)
HISTORICAL_BUILD_YEAR_RE = re.compile(r"(?<!\d)(2016|2017)(?!\d)")
SPINNER = "|/-\\"
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
INVALID_WIN_CHARS = {
    "<": "-",
    ">": "-",
    ":": ".",
    '"': "'",
    "/": "-",
    "\\": "-",
    "|": "-",
    "?": "-",
    "*": "-",
}
TREE_CACHE: dict[str, list[str]] = {}


class DownloadError(RuntimeError):
    pass


class CredentialError(RuntimeError):
    pass


class ManifestError(RuntimeError):
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


class CONSOLE_CURSOR_INFO(ctypes.Structure):
    _fields_ = [
        ("dwSize", ctypes.c_uint32),
        ("bVisible", ctypes.c_int),
    ]


@dataclass
class LocalBuild:
    path: Path
    name: str
    manifest_id: str
    launcher: str
    modified_ts: float
    preview: bool


@dataclass
class ManifestBundle:
    manifest_id: str
    beta_branch: str | None
    branch: str | None
    folder_name: str
    date_raw: str
    date_label: str
    safe_label: str
    patch_path: str | None
    patch_payload: dict | list | None
    patch_error: str | None = None
    local_folder: Path | None = None


@dataclass
class PatchResult:
    file_path: Path
    summary: str


class Noir:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    BLACK = "\033[30m"
    BG_BLACK = "\033[40m"
    ORANGE = "\033[38;5;208m"
    ORANGE_SOFT = "\033[38;5;214m"
    GOLD = "\033[38;5;220m"
    GREEN = "\033[38;5;82m"
    BLUE = "\033[38;5;39m"
    RED = "\033[38;5;196m"
    WHITE = "\033[38;5;255m"
    GRAY = "\033[38;5;245m"
    DARK = "\033[38;5;238m"

    use_color = True
    width = 74

    @classmethod
    def configure(cls, color: bool = True, width: int = 74) -> None:
        cls.use_color = color and not os.environ.get("NO_COLOR")
        cls.width = max(60, min(width, 100))
        if cls.use_color and os.name == "nt":
            os.system("")

    @classmethod
    def c(cls, color: str, text: str) -> str:
        if not cls.use_color:
            return text
        return f"{color}{text}{cls.RESET}"

    @classmethod
    def clear(cls) -> None:
        os.system("cls" if os.name == "nt" else "clear")

    @classmethod
    def line(cls, char: str = "-", color: str | None = None) -> None:
        print(cls.c(color or cls.DARK, char * cls.width))

    @classmethod
    def label(cls, text: str, color: str | None = None) -> str:
        return cls.c(color or cls.ORANGE, text)

    @classmethod
    def chip(cls, text: str, color: str | None = None) -> str:
        left = "["
        right = "]"
        return cls.c(color or cls.ORANGE_SOFT, f"{left}{text}{right}")

    @classmethod
    def header(cls, build_count: int, fake_mode: bool, storage: Path) -> None:
        mode = "PREVIEW" if fake_mode else "READY"
        title = "REC ROOM RELEASE"
        cls.line("=" , cls.ORANGE)
        print(cls.c(cls.BOLD + cls.ORANGE, title))
        cls.line("=" , cls.ORANGE)
        left = f"App {APP_ID} / Depot {DEPOT_ID}"
        right = f"{mode} / {build_count} builds"
        print(cls.c(cls.GRAY, left) + cls.c(cls.ORANGE_SOFT, right.rjust(max(1, cls.width - len(left)))))
        print(cls.c(cls.GRAY, "Storage ") + cls.c(cls.WHITE, str(storage)))
        cls.line(color=cls.DARK)

    @classmethod
    def section(cls, text: str) -> None:
        print()
        print(cls.c(cls.BOLD + cls.ORANGE, text.upper()))
        cls.line(color=cls.DARK)

    @classmethod
    def info(cls, text: str) -> None:
        print(f"{cls.chip('INFO', cls.ORANGE_SOFT)} {text}")

    @classmethod
    def blue_info(cls, text: str) -> None:
        print(f"{cls.chip('INFO', cls.BLUE)} {text}")

    @classmethod
    def ok(cls, text: str) -> None:
        print(f"{cls.chip('OK', cls.GREEN)} {text}")

    @classmethod
    def warn(cls, text: str) -> None:
        print(f"{cls.chip('WARN', cls.GOLD)} {text}")

    @classmethod
    def err(cls, text: str) -> None:
        print(f"{cls.chip('ERR', cls.RED)} {text}")

    @classmethod
    def menu(cls, rows: list[tuple[str, str]]) -> None:
        for key, title in rows:
            key_part = cls.c(cls.BOLD + cls.ORANGE, f"{key:>2}")
            title_part = cls.c(cls.WHITE, title)
            print(f" {key_part}  {title_part}")

    @classmethod
    def kv(cls, key: str, value: str) -> None:
        print(cls.c(cls.GRAY, f"{key:<10}") + cls.c(cls.WHITE, value))

    @classmethod
    def step(cls, label: str, result: str, detail: str = "", delay: float = STEP_DELAY) -> None:
        time.sleep(max(0.0, delay))
        dots = "." * max(1, 24 - len(label))
        print(
            cls.c(cls.GRAY, f"  {label} {dots} ")
            + cls.c(cls.GREEN, result)
            + (cls.c(cls.DIM + cls.GRAY, f"  {detail}") if detail else "")
        )


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def script_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def settings_path() -> Path:
    return script_dir() / REMEMBERING_NAME


def credentials_path() -> Path:
    return script_dir() / "DoNotShare"


def log_path() -> Path:
    return script_dir() / LOG_NAME


def error_log_path() -> Path:
    return script_dir() / ERROR_LOG_NAME


def depotdownloader_exe_path() -> Path:
    return script_dir() / "DepotDownloader.exe"


def remove_depotdownloader_package_dir() -> None:
    package_dir = script_dir() / "DepotDownloader"
    if package_dir.exists() and package_dir.is_dir():
        shutil.rmtree(package_dir, ignore_errors=True)


def depot_root(settings: dict | None = None) -> Path:
    root = (settings or {}).get("storage_root")
    if root:
        return Path(root)
    return script_dir() / "depots" / str(DEPOT_ID)


def preview_receipt_dir() -> Path:
    return script_dir() / ".release_noir"


def default_settings() -> dict:
    return {
        "state_version": PREVIEW_STATE_VERSION,
        "fake_mode": False,
        "theme": "orange-black",
        "created_at": now_iso(),
        "last_launch": None,
        "storage_root": str(script_dir() / "depots" / str(DEPOT_ID)),
        "app_update": {},
        "depotdownloader": {},
        "melonloader": {},
        "recent_manifests": [],
        "manifests": {},
    }


def normalize_manifest_id(value: object) -> str | None:
    if not isinstance(value, (str, int)):
        return None
    manifest_id = str(value).strip()
    if manifest_id and manifest_id.isascii() and manifest_id.isdecimal():
        return manifest_id
    return None


def compact_manifest_record(manifest_id: str, record: dict) -> dict | None:
    path_value = record.get("path")
    if not isinstance(path_value, str) or not path_value.strip():
        return None

    build_path = Path(path_value)
    if not build_path.exists() or not build_path.is_dir():
        return None

    compact: dict[str, str] = {"path": str(build_path)}
    beta_branch = record.get("beta_branch") or beta_branch_for_manifest(manifest_id)
    if isinstance(beta_branch, str) and beta_branch.strip():
        compact["beta_branch"] = beta_branch.strip()

    updated_at = record.get("updated_at")
    if isinstance(updated_at, str) and updated_at.strip():
        compact["updated_at"] = updated_at.strip()
    return compact


def prune_remembered_manifests(settings: dict) -> None:
    raw_manifests = settings.get("manifests")
    if not isinstance(raw_manifests, dict):
        raw_manifests = {}

    manifests: dict[str, dict] = {}
    for manifest_id, record in raw_manifests.items():
        if not isinstance(record, dict):
            continue
        normalized_manifest_id = normalize_manifest_id(manifest_id)
        if normalized_manifest_id is None:
            continue
        compact = compact_manifest_record(normalized_manifest_id, record)
        if compact is not None:
            manifests[normalized_manifest_id] = compact

    settings["manifests"] = manifests

    raw_recent = settings.get("recent_manifests")
    if not isinstance(raw_recent, list):
        raw_recent = []

    recent: list[str] = []
    for manifest_id in raw_recent:
        value = normalize_manifest_id(manifest_id)
        if value in manifests and value not in recent:
            recent.append(value)
    settings["recent_manifests"] = recent[:8]

    last_manifest = normalize_manifest_id(settings.get("last_manifest"))
    if last_manifest is not None and last_manifest in manifests:
        settings["last_manifest"] = last_manifest
        return
    if settings["recent_manifests"]:
        settings["last_manifest"] = settings["recent_manifests"][0]
    else:
        settings.pop("last_manifest", None)


def load_settings() -> dict:
    path = settings_path()
    if not path.exists():
        settings = default_settings()
        save_settings(settings)
        return settings

    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        backup = path.with_suffix(".broken.json")
        try:
            path.replace(backup)
        except OSError:
            pass
        settings = default_settings()
        save_settings(settings)
        return settings

    if not isinstance(loaded, dict):
        loaded = {}

    settings = default_settings()
    deep_update(settings, loaded)
    settings["state_version"] = PREVIEW_STATE_VERSION
    settings["fake_mode"] = False
    settings["last_launch"] = now_iso()
    settings.pop("fake_services", None)
    settings.pop("fake_steam_username", None)
    settings.pop("created_preview_builds", None)
    settings.pop("notes", None)
    prune_remembered_manifests(settings)
    save_settings(settings)
    return settings


def deep_update(base: dict, incoming: dict) -> None:
    for key, value in incoming.items():
        if isinstance(base.get(key), dict) and isinstance(value, dict):
            deep_update(base[key], value)
        else:
            base[key] = value


def save_settings(settings: dict) -> None:
    path = settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(settings, indent=2), encoding="utf-8")
    replace_with_retry(tmp, path)


def replace_with_retry(source: Path, target: Path) -> None:
    last_error: OSError | None = None
    for _ in range(8):
        try:
            source.replace(target)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.08)
    raise last_error if last_error is not None else OSError(f"Could not replace {target}")


def request_json(url: str) -> dict:
    req = request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": USER_AGENT,
        },
    )
    with request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
    if not isinstance(data, dict):
        raise DownloadError("GitHub returned an unexpected response.")
    return data


def request_bytes(url: str) -> bytes:
    req = request.Request(url, headers={"User-Agent": USER_AGENT})
    with request.urlopen(req, timeout=60) as resp:
        return resp.read()


def request_text(url: str) -> str:
    return request_bytes(url).decode("utf-8", errors="replace")


def normalize_version_tag(value: str) -> str:
    return value.strip().lstrip("vV")


def is_dev_version(value: str) -> bool:
    normalized = normalize_version_tag(value).lower()
    return not normalized or normalized.endswith("-dev") or "dev" in normalized


def parse_version_numbers(value: str) -> tuple[int, int, int]:
    normalized = normalize_version_tag(value).split("-", 1)[0]
    numbers: list[int] = []
    for chunk in normalized.split("."):
        if chunk.isdigit():
            numbers.append(int(chunk))
        else:
            match = re.match(r"(\d+)", chunk)
            numbers.append(int(match.group(1)) if match else 0)
        if len(numbers) == 3:
            break
    while len(numbers) < 3:
        numbers.append(0)
    return tuple(numbers[:3])


def is_outdated_version(current_version: str, latest_version: str) -> bool:
    return parse_version_numbers(current_version) < parse_version_numbers(latest_version)


def latest_app_release_api_url() -> str:
    return GITHUB_LATEST_RELEASE_API.format(owner=SELF_UPDATE_REPO_OWNER, repo=SELF_UPDATE_REPO_NAME)


def latest_app_release_page_url() -> str:
    return GITHUB_RELEASES_PAGE_URL.format(owner=SELF_UPDATE_REPO_OWNER, repo=SELF_UPDATE_REPO_NAME)


def read_latest_app_release() -> dict:
    payload = request_json(latest_app_release_api_url())
    tag_name = payload.get("tag_name")
    if not isinstance(tag_name, str) or not tag_name.strip():
        raise DownloadError("Latest GitHub release did not include a version tag.")
    html_url = payload.get("html_url")
    return {
        "version": normalize_version_tag(tag_name),
        "url": str(html_url) if isinstance(html_url, str) and html_url.strip() else latest_app_release_page_url(),
    }


def check_app_release(settings: dict, *, enforce: bool) -> int:
    current_version = normalize_version_tag(APP_VERSION)
    checked_at = now_iso()
    try:
        latest = read_latest_app_release()
    except (error.HTTPError, error.URLError, TimeoutError, DownloadError, json.JSONDecodeError) as exc:
        settings["app_update"] = {
            "checked_at": checked_at,
            "current": current_version or APP_VERSION,
            "status": "check_failed",
            "last_error": str(exc),
        }
        save_settings(settings)
        Noir.warn("GitHub update check failed; continuing.")
        return 0

    latest_version = str(latest["version"])
    release_url = str(latest["url"])
    dev_build = is_dev_version(current_version)
    update_required = (not dev_build) and is_outdated_version(current_version, latest_version)
    status = "dev" if dev_build else "outdated" if update_required else "current"
    settings["app_update"] = {
        "checked_at": checked_at,
        "current": current_version or APP_VERSION,
        "latest": latest_version,
        "status": status,
        "url": release_url,
    }
    save_settings(settings)

    if not (enforce and update_required):
        return 0

    Noir.clear()
    Noir.section("Update")
    Noir.err("This build is outdated.")
    Noir.kv("Current", f"v{current_version}")
    Noir.kv("Latest", f"v{latest_version}")
    Noir.kv("Download", release_url)
    try:
        webbrowser.open(release_url)
        Noir.ok("Opened GitHub releases.")
    except Exception as exc:
        Noir.warn(f"Could not open browser: {exc}")
    press_enter("Press Enter to close")
    return 2


def quote_repo_path(path: str) -> str:
    return parse.quote(path.replace("\\", "/"), safe="/")


def fetch_repo_file(branch: str, path: str) -> str:
    return request_text(
        GITHUB_RAW_FILE_URL.format(
            owner=PATCH_REPO_OWNER,
            repo=PATCH_REPO_NAME,
            branch=branch,
            path=quote_repo_path(path),
        )
    )


def try_fetch_repo_file(branch: str, path: str) -> str | None:
    try:
        return fetch_repo_file(branch, path)
    except error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def fetch_repo_bytes(branch: str, path: str) -> bytes:
    return request_bytes(
        GITHUB_RAW_FILE_URL.format(
            owner=PATCH_REPO_OWNER,
            repo=PATCH_REPO_NAME,
            branch=branch,
            path=quote_repo_path(path),
        )
    )


def get_repo_tree(branch: str) -> list[str]:
    if branch in TREE_CACHE:
        return TREE_CACHE[branch]
    payload = request_json(
        GITHUB_TREE_API.format(
            owner=PATCH_REPO_OWNER,
            repo=PATCH_REPO_NAME,
            branch=branch,
        )
    )
    entries = payload.get("tree")
    if not isinstance(entries, list):
        raise ManifestError("GitHub tree response was not usable.")
    paths = [str(item.get("path")) for item in entries if isinstance(item, dict) and item.get("path")]
    TREE_CACHE[branch] = paths
    return paths


def beta_branch_for_manifest(manifest_id: str) -> str | None:
    return BETA_MANIFESTS.get(manifest_id)


def manifest_lookup_name(manifest_id: str, beta_branch: str | None = None) -> str:
    return f"{manifest_id} {beta_branch}" if beta_branch else manifest_id


def choose_manifest_folder(manifest_id: str, folders: list[str], beta_branch: str | None = None) -> str:
    lookup_name = manifest_lookup_name(manifest_id, beta_branch)
    exact = [folder for folder in folders if folder == lookup_name]
    if exact:
        return exact[0]
    contains = [folder for folder in folders if lookup_name in folder]
    if len(contains) == 1:
        return contains[0]
    if not contains:
        raise ManifestError(f"Manifest folder was not found on GitHub: {lookup_name}")
    raise ManifestError(f"Manifest folder is ambiguous on GitHub: {lookup_name}")


def parse_date_json_text(text: str) -> str:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ManifestError("Date.json did not contain valid JSON.") from exc
    if not isinstance(value, str) or not value.strip():
        raise ManifestError("Date.json did not contain a usable date string.")
    return value.strip()


def parse_date_json_optional(text: str) -> str:
    try:
        return parse_date_json_text(text)
    except ManifestError:
        return ""


def format_manifest_date(raw_date: str) -> str:
    return raw_date.strip()


def make_windows_safe(name: str) -> str:
    cleaned = []
    for ch in name.strip():
        cleaned.append(INVALID_WIN_CHARS.get(ch, ch))
    value = "".join(cleaned).strip(" .")
    return value or "UnknownBuild"


def fallback_folder_label(manifest_id: str, beta_branch: str | None) -> str:
    return manifest_lookup_name(manifest_id, beta_branch)


def make_manifest_bundle(
    *,
    manifest_id: str,
    beta_branch: str | None,
    branch: str | None,
    folder_name: str,
    raw_date: str,
    patch_path: str | None,
    patch_payload: dict | list | None,
    patch_error: str | None = None,
    local_folder: Path | None = None,
) -> ManifestBundle:
    date_label = format_manifest_date(raw_date) if raw_date else "Unknown"
    folder_label = date_label if raw_date else fallback_folder_label(manifest_id, beta_branch)
    return ManifestBundle(
        manifest_id=manifest_id,
        beta_branch=beta_branch,
        branch=branch,
        folder_name=folder_name,
        date_raw=raw_date,
        date_label=date_label,
        safe_label=make_windows_safe(folder_label),
        patch_path=patch_path,
        patch_payload=patch_payload,
        patch_error=patch_error,
        local_folder=local_folder,
    )


def fallback_manifest_bundle(manifest_id: str) -> ManifestBundle:
    beta_branch = beta_branch_for_manifest(manifest_id)
    lookup_name = manifest_lookup_name(manifest_id, beta_branch)
    return make_manifest_bundle(
        manifest_id=manifest_id,
        beta_branch=beta_branch,
        branch=None,
        folder_name=lookup_name,
        raw_date="",
        patch_path=None,
        patch_payload=None,
        local_folder=None,
    )


def read_local_patch_payload(patch_path: Path, manifest_name: str) -> tuple[dict | list | None, str | None]:
    try:
        return json.loads(patch_path.read_text(encoding="utf-8")), None
    except json.JSONDecodeError as exc:
        return None, f"Patch.json was invalid for manifest {manifest_name}: {exc}"


def load_local_manifest_bundle(manifest_id: str) -> ManifestBundle | None:
    beta_branch = beta_branch_for_manifest(manifest_id)
    folder_name = manifest_lookup_name(manifest_id, beta_branch)
    folder = script_dir() / "manifest" / folder_name
    if not folder.exists() or not folder.is_dir():
        return None

    date_path = folder / "Date.json"
    patch_path = folder / "Patch.json"
    raw_date = parse_date_json_optional(date_path.read_text(encoding="utf-8")) if date_path.exists() else ""
    patch_payload, patch_error = read_local_patch_payload(patch_path, folder_name) if patch_path.exists() else (None, None)
    return make_manifest_bundle(
        manifest_id=manifest_id,
        beta_branch=beta_branch,
        branch=None,
        folder_name=folder_name,
        raw_date=raw_date,
        patch_path=str(patch_path) if patch_path.exists() else None,
        patch_payload=patch_payload,
        patch_error=patch_error,
        local_folder=folder,
    )


def manifest_folder_names(paths: list[str]) -> list[str]:
    folders: set[str] = set()
    for path in paths:
        normalized = path.replace("\\", "/")
        if not normalized.startswith("manifest/"):
            continue
        parts = normalized.split("/")
        if len(parts) >= 3 and parts[1]:
            folders.add(parts[1])
    return sorted(folders)


def lookup_manifest_bundle(manifest_id: str) -> ManifestBundle:
    local_bundle = load_local_manifest_bundle(manifest_id)
    if local_bundle is not None:
        return local_bundle

    beta_branch = beta_branch_for_manifest(manifest_id)
    exact_folder_name = manifest_lookup_name(manifest_id, beta_branch)
    for branch in PATCH_BRANCHES:
        try:
            folder_name = exact_folder_name
            date_path = f"manifest/{folder_name}/Date.json"
            patch_path = f"manifest/{folder_name}/Patch.json"
            date_text = try_fetch_repo_file(branch, date_path)
            if date_text is None:
                tree = get_repo_tree(branch)
                folders = manifest_folder_names(tree)
                folder_name = choose_manifest_folder(manifest_id, folders, beta_branch)
                date_path = f"manifest/{folder_name}/Date.json"
                patch_path = f"manifest/{folder_name}/Patch.json"
                date_text = fetch_repo_file(branch, date_path) if date_path in tree else None
            raw_date = parse_date_json_optional(date_text) if date_text is not None else ""
            patch_payload: dict | list | None = None
            patch_error: str | None = None
            patch_text = try_fetch_repo_file(branch, patch_path)
            if patch_text is not None:
                try:
                    patch_payload = json.loads(patch_text)
                except json.JSONDecodeError as exc:
                    patch_error = f"Patch.json was invalid for manifest {folder_name}: {exc}"
            return make_manifest_bundle(
                manifest_id=manifest_id,
                beta_branch=beta_branch,
                branch=branch,
                folder_name=folder_name,
                raw_date=raw_date,
                patch_path=patch_path if patch_text is not None else None,
                patch_payload=patch_payload,
                patch_error=patch_error,
                local_folder=None,
            )
        except Exception:
            continue

    return fallback_manifest_bundle(manifest_id)


def choose_depotdownloader_asset(release: dict) -> tuple[str, str, str]:
    tag = str(release.get("tag_name") or "").strip()
    assets = release.get("assets") or []
    scored: list[tuple[int, str, str]] = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        name = str(asset.get("name") or "")
        url = str(asset.get("browser_download_url") or "")
        if not name.lower().endswith(".zip") or not url:
            continue

        lower = name.lower()
        score = 0
        if "windows" in lower:
            score += 100
        if "win" in lower:
            score += 50
        if "x64" in lower or "amd64" in lower:
            score += 40
        if "arm" in lower:
            score -= 25
        if "linux" in lower or "macos" in lower or "osx" in lower:
            score -= 200
        scored.append((score, name, url))

    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    if not tag or not scored or scored[0][0] <= 0:
        raise DownloadError("Could not find a Windows DepotDownloader release.")
    _, asset_name, asset_url = scored[0]
    return tag, asset_name, asset_url


def melonloader_release_api_url() -> str:
    return MELONLOADER_RELEASE_API.format(tag=MELONLOADER_RELEASE_TAG)


def melonloader_download_url() -> str:
    return MELONLOADER_RELEASE_URL.format(
        tag=MELONLOADER_RELEASE_TAG,
        asset=parse.quote(MELONLOADER_ASSET_NAME),
    )


def choose_melonloader_asset(release: dict) -> tuple[str, str, str]:
    tag = str(release.get("tag_name") or "").strip()
    if normalize_version_tag(tag) != normalize_version_tag(MELONLOADER_RELEASE_TAG):
        raise DownloadError(f"MelonLoader release tag was not {MELONLOADER_RELEASE_TAG}.")

    assets = release.get("assets") or []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        name = str(asset.get("name") or "")
        url = str(asset.get("browser_download_url") or "")
        if name.lower() == MELONLOADER_ASSET_NAME.lower() and url:
            return tag, name, url
    raise DownloadError(f"{MELONLOADER_ASSET_NAME} was not found on the MelonLoader release.")


def resolve_melonloader_release() -> tuple[str, str, str]:
    try:
        return choose_melonloader_asset(request_json(melonloader_release_api_url()))
    except (error.HTTPError, error.URLError, TimeoutError, DownloadError, json.JSONDecodeError):
        return MELONLOADER_RELEASE_TAG, MELONLOADER_ASSET_NAME, melonloader_download_url()


def terminal_columns() -> int:
    return max(60, shutil.get_terminal_size((Noir.width, 20)).columns)


def set_console_cursor_visible(visible: bool) -> bool | None:
    if os.name == "nt":
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        info = CONSOLE_CURSOR_INFO()
        if not kernel32.GetConsoleCursorInfo(handle, ctypes.byref(info)):
            return None
        previous = bool(info.bVisible)
        info.bVisible = int(visible)
        if not kernel32.SetConsoleCursorInfo(handle, ctypes.byref(info)):
            return None
        return previous

    sys.stdout.write("\033[?25h" if visible else "\033[?25l")
    sys.stdout.flush()
    return None


def restore_console_cursor(previous: bool | None) -> None:
    if previous is None:
        set_console_cursor_visible(True)
    else:
        set_console_cursor_visible(previous)


def render_one_line(text: str, last_len: int = 0) -> int:
    width = terminal_columns()
    clean = " ".join(text.replace("\t", " ").split())
    if len(clean) > width - 1:
        clean = clean[: max(1, width - 4)] + "..."
    padding = " " * max(0, last_len - len(clean))
    sys.stdout.write("\r" + clean + padding)
    sys.stdout.flush()
    return len(clean)


def clear_rendered_line(last_len: int) -> None:
    if last_len <= 0:
        return
    sys.stdout.write("\r" + (" " * last_len) + "\r")
    sys.stdout.flush()


def download_file(url: str, dest: Path, label: str) -> None:
    req = request.Request(url, headers={"User-Agent": USER_AGENT})
    last_len = 0
    spinner_index = 0
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
                spinner_index = (spinner_index + 1) % len(SPINNER)
                spin = Noir.c(Noir.ORANGE, SPINNER[spinner_index])
                if total:
                    percent = (read / total) * 100
                    line = f"{spin} {percent:6.2f}% {label}"
                else:
                    line = f"{spin} {read // 1024:>7} KB {label}"
                last_len = render_one_line(line, last_len)
    sys.stdout.write("\n")


def safe_extract_zip(zip_path: Path, target_dir: Path) -> None:
    target_root = target_dir.resolve()
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            member_name = member.filename.replace("\\", "/")
            if not member_name or Path(member_name).is_absolute():
                raise DownloadError("Release archive contained an unsafe path.")
            destination = (target_dir / member_name).resolve()
            try:
                destination.relative_to(target_root)
            except ValueError as exc:
                raise DownloadError("Release archive contained an unsafe path.") from exc
        zf.extractall(target_dir)


def clean_work_dir(work_dir: Path) -> None:
    if not work_dir.exists():
        return
    if work_dir.is_dir():
        shutil.rmtree(work_dir, ignore_errors=True)
    else:
        work_dir.unlink()


def install_melonloader_to_build(build_dir: Path, settings: dict | None = None) -> None:
    if not build_dir.exists() or not build_dir.is_dir():
        raise DownloadError(f"Build folder was not found: {build_dir}")

    tag, asset_name, asset_url = resolve_melonloader_release()
    work_dir = build_dir / ".melonloader_install"
    zip_path = work_dir / asset_name
    clean_work_dir(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    Noir.section("MelonLoader")
    try:
        download_file(asset_url, zip_path, asset_name)
        try:
            safe_extract_zip(zip_path, build_dir)
        except zipfile.BadZipFile as exc:
            raise DownloadError("MelonLoader archive was invalid.") from exc
    finally:
        clean_work_dir(work_dir)

    if settings is not None:
        settings.setdefault("melonloader", {}).update(
            {
                "version": tag,
                "asset": asset_name,
                "installed_at": now_iso(),
                "last_build": str(build_dir),
            }
        )
        save_settings(settings)
    Noir.ok(f"MelonLoader {tag} installed to {build_dir}")


def install_depotdownloader_release(settings: dict, tag: str, asset_name: str, asset_url: str) -> None:
    work_dir = script_dir() / ".depotdownloader_update"
    zip_path = work_dir / asset_name
    extract_dir = work_dir / "extract"
    if work_dir.exists():
        shutil.rmtree(work_dir, ignore_errors=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    Noir.section("DepotDownloader")
    try:
        download_file(asset_url, zip_path, asset_name)

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)
        except zipfile.BadZipFile as exc:
            raise DownloadError("DepotDownloader archive was invalid.") from exc

        matches = list(extract_dir.rglob("DepotDownloader.exe"))
        if not matches:
            raise DownloadError("DepotDownloader.exe was not found in the release archive.")

        shutil.copy2(matches[0], depotdownloader_exe_path())
        remove_depotdownloader_package_dir()
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

    settings["depotdownloader"] = {
        "version": tag,
        "asset": asset_name,
        "exe": str(depotdownloader_exe_path()),
        "checked_at": now_iso(),
        "updated_at": now_iso(),
    }
    save_settings(settings)
    Noir.ok(f"DepotDownloader {tag}")


def ensure_depotdownloader(settings: dict) -> None:
    remove_depotdownloader_package_dir()
    try:
        release = request_json(DEPOTDOWNLOADER_RELEASE_API)
        tag, asset_name, asset_url = choose_depotdownloader_asset(release)
    except (error.HTTPError, error.URLError, TimeoutError, DownloadError) as exc:
        if depotdownloader_exe_path().exists():
            settings.setdefault("depotdownloader", {})["checked_at"] = now_iso()
            settings["depotdownloader"]["last_error"] = str(exc)
            save_settings(settings)
            Noir.warn("DepotDownloader update check failed; using local copy.")
            return
        raise DownloadError(f"DepotDownloader could not be downloaded: {exc}") from exc

    info = settings.setdefault("depotdownloader", {})
    info["checked_at"] = now_iso()
    if depotdownloader_exe_path().exists() and info.get("version") == tag and info.get("asset") == asset_name:
        info["exe"] = str(depotdownloader_exe_path())
        info.pop("last_error", None)
        save_settings(settings)
        return

    install_depotdownloader_release(settings, tag, asset_name, asset_url)


def _blob_from_bytes(data: bytes):
    keepalive = ctypes.create_string_buffer(data)
    blob = DATA_BLOB(len(data), ctypes.cast(keepalive, ctypes.POINTER(ctypes.c_ubyte)))
    return blob, keepalive


def _bytes_from_blob(blob: DATA_BLOB) -> bytes:
    if not blob.cbData:
        return b""
    return ctypes.string_at(blob.pbData, blob.cbData)


def protect_text(text: str) -> dict:
    raw = text.encode("utf-8")
    if os.name != "nt":
        return {"method": "base64", "value": base64.b64encode(raw).decode("ascii")}

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    data_in, keepalive = _blob_from_bytes(raw)
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
        raise CredentialError(f"CryptProtectData failed: {ctypes.GetLastError()}")
    try:
        return {"method": "dpapi", "value": base64.b64encode(_bytes_from_blob(data_out)).decode("ascii")}
    finally:
        if data_out.pbData:
            kernel32.LocalFree(data_out.pbData)
        del keepalive


def unprotect_text(payload: dict) -> str:
    method = payload.get("method")
    value = payload.get("value")
    if not isinstance(value, str):
        raise CredentialError("Saved credential payload is incomplete.")

    raw = base64.b64decode(value.encode("ascii"))
    if method == "base64":
        return raw.decode("utf-8")
    if method != "dpapi":
        raise CredentialError("Saved credential payload uses an unknown method.")
    if os.name != "nt":
        raise CredentialError("Saved credentials require Windows DPAPI.")

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
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
        raise CredentialError(f"CryptUnprotectData failed: {ctypes.GetLastError()}")
    try:
        return _bytes_from_blob(data_out).decode("utf-8")
    finally:
        if data_out.pbData:
            kernel32.LocalFree(data_out.pbData)
        del keepalive


def load_credential_state() -> dict:
    path = credentials_path()
    if not path.exists():
        return {"version": 1, "reuse_credentials": True}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise CredentialError(f"Could not read DoNotShare: {exc}") from exc
    return data if isinstance(data, dict) else {"version": 1, "reuse_credentials": True}


def save_credential_state(state: dict) -> None:
    state["version"] = 1
    state["updated_at"] = now_iso()
    path = credentials_path()
    tmp = path.with_name(f"DoNotShare.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    replace_with_retry(tmp, path)


def has_saved_credentials(state: dict) -> bool:
    return isinstance(state.get("username"), dict) and isinstance(state.get("password"), dict)


def prompt_password(label: str) -> str:
    prompt_text = Noir.c(Noir.ORANGE_SOFT, f"{label}: ")
    if os.name != "nt":
        return getpass.getpass(prompt_text)

    import msvcrt

    sys.stdout.write(prompt_text)
    sys.stdout.flush()
    chars: list[str] = []
    while True:
        key = msvcrt.getwch()
        if key in {"\r", "\n"}:
            print()
            return "".join(chars)
        if key == "\003":
            print()
            raise KeyboardInterrupt
        if key == "\b":
            if chars:
                chars.pop()
                sys.stdout.write("\b \b")
                sys.stdout.flush()
            continue
        if key in {"\x00", "\xe0"}:
            msvcrt.getwch()
            continue
        if key.isprintable():
            chars.append(key)
            sys.stdout.write("*")
            sys.stdout.flush()


def save_credentials(username: str, password: str, reuse: bool = True) -> None:
    save_credential_state(
        {
            "version": 1,
            "reuse_credentials": reuse,
            "username": protect_text(username),
            "password": protect_text(password),
            "created_at": now_iso(),
        }
    )


def read_saved_credentials(state: dict) -> tuple[str, str]:
    username = unprotect_text(state["username"])
    password = unprotect_text(state["password"])
    return username, password


def prompt_for_credentials() -> tuple[str, str]:
    username = prompt("Steam username")
    if not username:
        raise CredentialError("Steam username is required.")
    password = prompt_password("Steam password")
    if not password:
        raise CredentialError("Steam password is required.")
    save_credentials(username, password, reuse=True)
    return username, password


def ensure_credentials_for_download() -> tuple[str, str] | None:
    state = load_credential_state()
    if has_saved_credentials(state):
        if not state.get("reuse_credentials", True):
            Noir.warn("Saved login reuse is disabled in Settings.")
            return None

        if prompt_choice({"y", "n"}, "Reuse saved Steam login? y/n") == "y":
            try:
                return read_saved_credentials(state)
            except CredentialError as exc:
                Noir.warn(str(exc))
                return prompt_for_credentials()

        state["reuse_credentials"] = False
        save_credential_state(state)
        Noir.warn("Saved login reuse disabled. Re-enable it in Settings.")
        return None

    return prompt_for_credentials()


def mask_command(args: list[str]) -> str:
    masked: list[str] = []
    hide_next = False
    for index, part in enumerate(args):
        if hide_next:
            masked.append("********")
            hide_next = False
            continue
        if index == 0 and Path(part) == depotdownloader_exe_path():
            masked.append(".\\DepotDownloader.exe")
        else:
            masked.append(part)
        if part in {"-username", "-password"}:
            hide_next = True
    return " ".join(masked)


def parse_percent(line: str) -> float | None:
    matches = PERCENT_RE.findall(line)
    if not matches:
        return None
    try:
        return float(matches[-1])
    except ValueError:
        return None


def format_percent(percent: float | None) -> str:
    if percent is None:
        return ""
    return f"{percent:.2f}%"


def normalize_console_line(text: str) -> str:
    return " ".join(text.strip().split())


def split_steam_guard_prompt(text: str) -> tuple[str, str, str] | None:
    match = STEAM_GUARD_PROMPT_RE.search(text)
    if not match:
        return None
    prompt_text = normalize_console_line(match.group(0))
    return text[: match.start()], prompt_text, text[match.end() :]


def prompt_steam_guard_code(prompt_text: str) -> str:
    print(Noir.c(Noir.GRAY, prompt_text))
    while True:
        code = prompt_password("Steam Guard code").strip()
        if code:
            return code
        Noir.warn("Steam Guard code is required.")


def remove_leading_percent(line: str) -> str:
    return LEADING_PERCENT_RE.sub("", line, count=1).strip()


def dash_variants(text: str) -> set[str]:
    variants = {text}
    dash_pairs = [
        (" - ", " \u2013 "),
        (" - ", " \u2014 "),
        (" - ", " \u2212 "),
    ]
    for plain, fancy in dash_pairs:
        if plain in text:
            variants.add(text.replace(plain, fancy))
        if fancy in text:
            variants.add(text.replace(fancy, plain))
    return variants


def path_display_roots(root: Path | None) -> list[str]:
    if root is None:
        return []

    roots: list[str] = []
    for candidate in (root, root.resolve()):
        text = str(candidate)
        roots.extend((text, text.replace("\\", "/")))
        try:
            relative = candidate.relative_to(script_dir())
        except ValueError:
            continue
        relative_text = str(relative)
        roots.extend((relative_text, relative_text.replace("\\", "/")))
    roots.append(root.name)

    expanded_roots: set[str] = set()
    for item in roots:
        for variant in dash_variants(item):
            expanded_roots.add(variant)
            expanded_roots.add(variant.replace("\\", "/"))

    deduped = {item.rstrip("\\/") for item in expanded_roots if item and item not in {".", "./"}}
    return sorted(deduped, key=len, reverse=True)


def find_path_start(text: str, index: int) -> int:
    drive_matches = list(re.finditer(r"[A-Za-z]:[/\\]", text[:index]))
    if drive_matches:
        return drive_matches[-1].start()

    start = index
    while start > 0 and text[start - 1] not in {" ", "\t", "\"", "'", "(", "[", "<"}:
        start -= 1
    return start


def shorten_depot_prefixed_paths(line: str) -> tuple[str, bool]:
    result = line
    shortened = False
    marker = f"depots/{DEPOT_ID}/"

    while True:
        normalized = result.replace("\\", "/")
        index = normalized.lower().find(marker)
        if index < 0:
            return result, shortened

        folder_start = index + len(marker)
        folder_end = normalized.find("/", folder_start)
        if folder_end < 0:
            folder_end = len(result)
        else:
            folder_end += 1

        path_start = find_path_start(result, index)
        result = result[:path_start] + ".\\" + result[folder_end:]
        shortened = True


def shorten_known_inner_paths(line: str) -> tuple[str, bool]:
    result = line
    shortened = False
    markers = (
        ".depotdownloader/",
        "recroom_release_data/",
        "recroom_data/",
        "recroom_release.exe",
        "recroom.exe",
    )

    while True:
        normalized = result.replace("\\", "/")
        lower = normalized.lower()
        found = [(lower.find(marker), marker) for marker in markers if lower.find(marker) >= 0]
        if not found:
            return result, shortened

        marker_index, _ = min(found, key=lambda item: item[0])
        if marker_index >= 2 and result[marker_index - 2:marker_index] in {"./", ".\\"}:
            return result, shortened

        path_start = find_path_start(result, marker_index)
        result = result[:path_start] + ".\\" + result[marker_index:]
        shortened = True


def arg_value(args: list[str], flag: str) -> str | None:
    try:
        index = args.index(flag)
    except ValueError:
        return None
    if index + 1 >= len(args):
        return None
    return args[index + 1]


def shorten_depot_detail(line: str, display_root: Path | None) -> str:
    result = line
    shortened = False
    for root_text in path_display_roots(display_root):
        needle = root_text.lower()
        search_from = 0
        while True:
            lower = result.lower()
            index = lower.find(needle, search_from)
            if index < 0:
                break
            before = result[index - 1] if index > 0 else ""
            after_index = index + len(root_text)
            after = result[after_index] if after_index < len(result) else ""
            if before and before not in {" ", "\t", "\"", "'", "(", "["}:
                search_from = index + len(root_text)
                continue
            if after and after not in {"\\", "/", " ", "\t", "\"", "'", ")", "]", ".", ","}:
                search_from = index + len(root_text)
                continue
            tail_index = after_index + 1 if after in {"\\", "/"} else after_index
            result = result[:index] + ".\\" + result[tail_index:]
            shortened = True
            search_from = index + 2
    result, depot_shortened = shorten_depot_prefixed_paths(result)
    result, inner_shortened = shorten_known_inner_paths(result)
    shortened = shortened or depot_shortened or inner_shortened
    return result.replace("/", "\\") if shortened else result


def stream_depotdownloader(args: list[str], display_root: Path | None = None) -> int:
    if display_root is None:
        dir_arg = arg_value(args, "-dir")
        if dir_arg:
            display_root = Path(dir_arg)

    try:
        process = subprocess.Popen(
            args,
            cwd=str(script_dir()),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=True,
            bufsize=0,
            errors="replace",
        )
    except FileNotFoundError as exc:
        raise DownloadError(f"Could not start DepotDownloader: {args[0]}") from exc
    assert process.stdout is not None
    assert process.stdin is not None

    captured = [mask_command(args)]
    buffer = ""
    spinner_index = 0
    last_percent: float | None = None
    last_len = 0

    def flush_line(raw: str) -> None:
        nonlocal spinner_index, last_percent, last_len
        line = normalize_console_line(raw)
        if not line:
            return
        percent = parse_percent(line)
        if percent is not None and percent != last_percent:
            spinner_index = (spinner_index + 1) % len(SPINNER)
            last_percent = percent
        spin = Noir.c(Noir.ORANGE, SPINNER[spinner_index])
        percent_text = format_percent(percent)
        detail = remove_leading_percent(line) if percent is not None else line
        detail = shorten_depot_detail(detail, display_root)
        parts = [spin]
        if percent_text:
            parts.append(percent_text)
        if detail:
            parts.append(detail)
        last_len = render_one_line(" ".join(parts), last_len)
        captured.append(line)

    def submit_steam_guard_code(prompt_text: str) -> None:
        nonlocal last_len
        clear_rendered_line(last_len)
        sys.stdout.write("\n")
        sys.stdout.flush()
        typing_cursor_state = set_console_cursor_visible(True)
        try:
            code = prompt_steam_guard_code(prompt_text)
        finally:
            restore_console_cursor(typing_cursor_state)
        try:
            process.stdin.write(code + "\n")
            process.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise DownloadError("DepotDownloader asked for Steam Guard, but its input pipe closed.") from exc
        captured.append(prompt_text)
        captured.append("Steam Guard code: ********")
        last_len = 0

    cursor_state = set_console_cursor_visible(False)
    try:
        while True:
            char = process.stdout.read(1)
            if char == "":
                if buffer:
                    flush_line(buffer)
                if process.poll() is not None:
                    break
                time.sleep(0.01)
                continue
            if char in {"\r", "\n"}:
                flush_line(buffer)
                buffer = ""
                continue
            buffer += char
            while True:
                prompt_parts = split_steam_guard_prompt(buffer)
                if not prompt_parts:
                    break
                before_prompt, prompt_text, after_prompt = prompt_parts
                if before_prompt.strip():
                    flush_line(before_prompt)
                submit_steam_guard_code(prompt_text)
                buffer = after_prompt

        exit_code = process.wait()
        sys.stdout.write("\n")
    finally:
        restore_console_cursor(cursor_state)
    captured.append(f"exit_code={exit_code}")
    log_path().write_text("\n".join(captured), encoding="utf-8", errors="replace")
    return exit_code


def get_instruction_base_dir(item: dict) -> str | None:
    for key in ("base_dir", "base", "root", "base_path"):
        value = item.get(key)
        if value is None:
            continue
        if not isinstance(value, str):
            raise PatchError(f"{key} must be a string.")
        cleaned = value.strip()
        return cleaned or None
    return None


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
            raise PatchError("Every patch instruction must be an object.")

        kind = str(item.get("type") or item.get("kind") or "replace_bytes").strip().lower()
        file_value = item.get("file") or item.get("path") or item.get("target") or item.get("destination") or item.get("to")
        source_value = item.get("source") or item.get("src") or item.get("from")
        base_dir_value = get_instruction_base_dir(item)

        if kind in {"copy_patch_file", "copy_file_from_patch", "install_patch_file", "move_patch_file", "move_file_from_patch"}:
            if not isinstance(file_value, str) or not file_value.strip():
                raise PatchError("Patch file install instructions need a target file.")
            if not isinstance(source_value, str) or not source_value.strip():
                raise PatchError("Patch file install instructions need a source file.")
            normalized.append(
                {
                    "type": kind,
                    "file": file_value.strip(),
                    "source": source_value.strip(),
                    "overwrite": bool(item.get("overwrite", True)),
                    "base_dir": base_dir_value,
                }
            )
            continue

        if not isinstance(file_value, str) or not file_value.strip():
            raise PatchError("Patch instruction needs a target file.")

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
                    "base_dir": base_dir_value,
                }
            )
            continue

        replacements = item.get("replacements")
        if replacements is None:
            find_value = item.get("find")
            replace_value = item.get("replace")
            if isinstance(find_value, str) and isinstance(replace_value, str):
                replacements = [{"find": find_value, "replace": replace_value}]
        if not isinstance(replacements, list) or not replacements:
            raise PatchError("Patch instruction needs replacements.")

        normalized.append(
            {
                "type": kind,
                "file": file_value.strip(),
                "base_dir": base_dir_value,
                "replacements": replacements,
                "encoding": str(item.get("encoding") or "utf-8"),
            }
        )

    return normalized


def resolve_base_dir(build_dir: Path, base_dir: str | None, *, allow_create: bool = False) -> Path:
    if base_dir is None or not base_dir.strip() or base_dir.strip() == ".":
        return build_dir
    relative = Path(base_dir.strip())
    if relative.is_absolute():
        raise PatchError(f"base_dir must be relative: {base_dir}")
    candidate = build_dir / relative
    if candidate.exists() and candidate.is_dir():
        return candidate
    if allow_create:
        return candidate
    raise PatchError(f"Patch base directory was not found: {base_dir}")


def resolve_existing_target(base_dir: Path, file_value: str) -> Path:
    relative = Path(file_value)
    if relative.is_absolute():
        raise PatchError(f"Patch target must be relative: {file_value}")
    direct = base_dir / relative
    if direct.exists() and direct.is_file():
        return direct

    matches = [path for path in base_dir.rglob(relative.name) if path.is_file()]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise PatchError(f"Patch target file was not found: {file_value}")
    raise PatchError(f"Patch target file is ambiguous: {file_value}")


def resolve_patch_target(build_dir: Path, file_value: str, kind: str, base_dir: str | None) -> Path:
    if kind in {"write_text_file", "create_text_file", "write_text", "install_patch_file", "copy_patch_file", "copy_file_from_patch", "move_patch_file", "move_file_from_patch"}:
        base = resolve_base_dir(build_dir, base_dir, allow_create=kind in {"write_text_file", "create_text_file", "write_text"})
        relative = Path(file_value)
        if relative.is_absolute():
            raise PatchError(f"Patch target must be relative: {file_value}")
        return base / relative

    base = resolve_base_dir(build_dir, base_dir)
    return resolve_existing_target(base, file_value)


def resolve_patch_source(bundle: ManifestBundle, source_value: str) -> bytes:
    relative = Path(source_value)
    if relative.is_absolute():
        raise PatchError(f"Patch source must be relative: {source_value}")

    if bundle.local_folder is not None:
        source_path = bundle.local_folder / relative
        if not source_path.exists() or not source_path.is_file():
            raise PatchError(f"Patch source file was not found: {source_value}")
        return source_path.read_bytes()

    if bundle.branch is None:
        raise PatchError(f"Patch source file cannot be resolved: {source_value}")
    source_path = (Path("manifest") / bundle.folder_name / relative).as_posix()
    try:
        return fetch_repo_bytes(bundle.branch, source_path)
    except Exception as exc:
        raise PatchError(f"Patch source file could not be downloaded: {source_value}") from exc


def apply_replace_bytes(file_path: Path, replacements: list[dict], encoding: str) -> int:
    data = file_path.read_bytes()
    total = 0
    for index, replacement in enumerate(replacements, start=1):
        if not isinstance(replacement, dict):
            raise PatchError(f"Replacement #{index} is not an object.")
        find_value = replacement.get("find")
        replace_value = replacement.get("replace")
        if not isinstance(find_value, str) or not isinstance(replace_value, str):
            raise PatchError(f"Replacement #{index} needs string find and replace values.")
        find_bytes = find_value.encode(encoding)
        replace_bytes = replace_value.encode(encoding)
        if len(find_bytes) != len(replace_bytes):
            raise PatchError(f"Replacement #{index} changes byte length in {file_path.name}.")
        count = data.count(find_bytes)
        if count <= 0:
            raise PatchError(f"Replacement #{index} was not found in {file_path.name}.")
        data = data.replace(find_bytes, replace_bytes)
        total += count
    file_path.write_bytes(data)
    return total


def write_text_file(target_path: Path, content: str, encoding: str, overwrite: bool) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and target_path.is_dir():
        raise PatchError(f"Patch target is a directory: {target_path}")
    if target_path.exists() and not overwrite:
        raise PatchError(f"Patch target already exists: {target_path.name}")
    target_path.write_text(content, encoding=encoding, newline="")


def install_patch_file(build_dir: Path, bundle: ManifestBundle, instruction: dict) -> Path:
    target_path = resolve_patch_target(
        build_dir,
        instruction["file"],
        instruction["type"],
        instruction.get("base_dir"),
    )
    if target_path.exists() and target_path.is_dir():
        raise PatchError(f"Patch target is a directory: {target_path}")
    if target_path.exists() and not instruction.get("overwrite", True):
        raise PatchError(f"Patch target already exists: {target_path.name}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    source_bytes = resolve_patch_source(bundle, instruction["source"])
    target_path.write_bytes(source_bytes)
    return target_path


def apply_patch_payload(build_dir: Path, bundle: ManifestBundle) -> list[PatchResult]:
    if bundle.patch_error:
        Noir.section("Patch")
        raise PatchError(bundle.patch_error)

    if bundle.patch_payload is None:
        Noir.section("Patch")
        Noir.warn("No Patch.json found. Download kept as-is.")
        return []

    instructions = normalize_patch_instructions(bundle.patch_payload)
    results: list[PatchResult] = []
    if not instructions:
        raise PatchError("Patch.json did not contain any instructions.")

    Noir.section("Patch")
    for instruction in instructions:
        kind = instruction["type"]
        if kind in {"copy_patch_file", "copy_file_from_patch", "install_patch_file", "move_patch_file", "move_file_from_patch"}:
            target = install_patch_file(build_dir, bundle, instruction)
            Noir.ok(f"{target.relative_to(build_dir)}")
            results.append(PatchResult(target, "patch file installed"))
            continue

        target = resolve_patch_target(build_dir, instruction["file"], kind, instruction.get("base_dir"))
        if kind in {"write_text_file", "create_text_file", "write_text"}:
            write_text_file(target, instruction["content"], instruction["encoding"], instruction["overwrite"])
            Noir.ok(f"{target.relative_to(build_dir)}")
            results.append(PatchResult(target, "text file written"))
            continue

        if kind not in {"replace_bytes", "replace_text", "replace_strings"}:
            raise PatchError(f"Unsupported patch type: {kind}")

        count = apply_replace_bytes(target, instruction["replacements"], instruction["encoding"])
        Noir.ok(f"{target.relative_to(build_dir)} ({count})")
        results.append(PatchResult(target, f"{count} replacement(s)"))

    return results


def append_unique_recent(settings: dict, manifest_id: str) -> None:
    recent = [item for item in settings.get("recent_manifests", []) if item != manifest_id]
    recent.insert(0, manifest_id)
    settings["recent_manifests"] = recent[:8]


def prompt(label: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    try:
        value = input(Noir.c(Noir.ORANGE_SOFT, f"{label}{suffix}: ")).strip()
    except EOFError:
        return default or ""
    return value or default or ""


def prompt_choice(valid: set[str], label: str = "Select") -> str:
    while True:
        value = prompt(label).lower()
        if value in valid:
            return value
        Noir.warn("That option is not available.")


def press_enter(message: str = "Press Enter to continue") -> None:
    try:
        input(Noir.c(Noir.GRAY, f"\n{message} . . ."))
    except EOFError:
        pass


def safe_name(value: str) -> str:
    cleaned = []
    for ch in value.strip():
        if ch.isalnum() or ch in {" ", ".", "-", "_", "(", ")"}:
            cleaned.append(ch)
        else:
            cleaned.append("-")
    final = "".join(cleaned).strip(" .-_")
    while "  " in final:
        final = final.replace("  ", " ")
    return final or "UnknownBuild"


def make_unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    counter = 2
    while True:
        candidate = path.with_name(f"{path.name} ({counter})")
        if not candidate.exists():
            return candidate
        counter += 1


def manifest_download_path(settings: dict, bundle: ManifestBundle) -> Path:
    return depot_root(settings) / bundle.safe_label


def remembered_manifest_path(settings: dict, manifest_id: str) -> Path | None:
    record = settings.get("manifests", {}).get(manifest_id)
    if not isinstance(record, dict):
        return None
    path_value = record.get("path")
    if not isinstance(path_value, str) or not path_value.strip():
        return None
    return Path(path_value)


def is_within_directory(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def existing_manifest_paths(settings: dict, bundle: ManifestBundle) -> list[Path]:
    candidates = [
        manifest_download_path(settings, bundle),
        depot_root(settings) / bundle.manifest_id,
        depot_root(settings) / bundle.folder_name,
    ]
    remembered = remembered_manifest_path(settings, bundle.manifest_id)
    if remembered is not None:
        candidates.append(remembered)

    found: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate.exists():
            continue
        key = str(candidate.resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        found.append(candidate)
    return found


def existing_manifest_menu(settings: dict, bundle: ManifestBundle) -> str:
    paths = existing_manifest_paths(settings, bundle)
    if not paths:
        return "download"

    Noir.section("Already Downloaded")
    Noir.kv("Manifest", bundle.manifest_id)
    Noir.kv("Date", bundle.date_label)
    Noir.kv("Path", str(paths[0]))
    if len(paths) > 1:
        Noir.kv("Also", f"{len(paths) - 1} more")
    Noir.menu(
        [
            ("R", "Replace"),
            ("B", "Back"),
        ]
    )
    Noir.line(color=Noir.DARK)
    while True:
        choice = prompt("Select").lower()
        if choice == "r":
            return "replace"
        if choice in {"0", "b", "back"}:
            return "back"
        Noir.warn("Choose R or Back.")


def replace_manifest_download(settings: dict, bundle: ManifestBundle) -> None:
    root = depot_root(settings)
    for path in existing_manifest_paths(settings, bundle):
        if not is_within_directory(path, root):
            raise DownloadError(f"Refusing to replace a path outside storage: {path}")
        if path.resolve() == root.resolve():
            raise DownloadError(f"Refusing to replace storage root: {path}")
        if not path.is_dir():
            raise DownloadError(f"Manifest path is not a folder: {path}")
        shutil.rmtree(path)


def files_are_identical(left: Path, right: Path) -> bool:
    try:
        if left.stat().st_size != right.stat().st_size:
            return False
        with left.open("rb") as left_handle, right.open("rb") as right_handle:
            while True:
                left_chunk = left_handle.read(DOWNLOAD_CHUNK)
                right_chunk = right_handle.read(DOWNLOAD_CHUNK)
                if left_chunk != right_chunk:
                    return False
                if not left_chunk:
                    return True
    except OSError:
        return False


def move_directory_contents(source_dir: Path, target_dir: Path) -> None:
    for item in list(source_dir.iterdir()):
        target = target_dir / item.name
        if target.exists():
            if item.is_dir() and target.is_dir():
                move_directory_contents(item, target)
                try:
                    item.rmdir()
                except OSError as exc:
                    raise DownloadError(f"Could not remove merged folder: {item}") from exc
                continue
            if item.is_file() and target.is_file() and files_are_identical(item, target):
                item.unlink()
                continue
            raise DownloadError(f"Beta layout conflict while moving {item.name}.")
        shutil.move(str(item), str(target))


def normalize_beta_download_layout(build_dir: Path, bundle: ManifestBundle) -> None:
    if not bundle.beta_branch:
        return

    zero_dir = build_dir / "0"
    if not zero_dir.exists():
        return
    if not zero_dir.is_dir():
        raise DownloadError(f"Beta layout path is not a folder: {zero_dir}")

    Noir.section("Layout")
    Noir.info("Moving beta files out of .\\0")
    move_directory_contents(zero_dir, build_dir)
    try:
        zero_dir.rmdir()
    except OSError as exc:
        raise DownloadError(f"Could not remove empty beta folder: {zero_dir}") from exc
    Noir.ok("Beta layout normalized.")


def clean_build_metadata(build_dir: Path) -> None:
    metadata_dir = build_dir / ".DepotDownloader"
    if metadata_dir.exists():
        if not metadata_dir.is_dir():
            raise DownloadError(f"Build metadata path is not a folder: {metadata_dir}")
        shutil.rmtree(metadata_dir)

    for marker_name in (".release_noir_manifest.json", ".release_noir_preview.json"):
        marker = build_dir / marker_name
        if marker.exists():
            if marker.is_dir():
                raise DownloadError(f"Build metadata path is a folder: {marker}")
            marker.unlink()


def find_launcher_name(build_dir: Path) -> str:
    launcher = find_launch_executable(build_dir)
    if launcher is not None:
        return launcher.name
    return "pending"


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


def historical_year_from_text(value: str) -> int | None:
    match = HISTORICAL_BUILD_YEAR_RE.search(value)
    return int(match.group(1)) if match else None


def historical_build_year(build: LocalBuild) -> int | None:
    for value in (build.name, build.path.name):
        year = historical_year_from_text(value)
        if year is not None:
            return year

    manifest_id = normalize_manifest_id(build.manifest_id)
    if manifest_id is None:
        return None
    try:
        bundle = lookup_manifest_bundle(manifest_id)
    except Exception:
        return None
    for value in (bundle.date_raw, bundle.date_label, bundle.safe_label):
        year = historical_year_from_text(value)
        if year is not None:
            return year
    return None


def is_historical_melonloader_build(build: LocalBuild) -> bool:
    return historical_build_year(build) in {2016, 2017}


def melonloader_policy_label(policy: object) -> str:
    value = melonloader_policy_value(policy)
    if value == "always_install":
        return "Install every time"
    if value == "never_install":
        return "Reject"
    return "Ask every time"


def melonloader_policy_value(policy: object) -> str:
    value = str(policy or "ask")
    if value == "reject":
        return "never_install"
    if value == "install":
        return "always_install"
    if value in {"always_install", "never_install"}:
        return value
    return "ask"


def current_melonloader_policy(settings: dict) -> str:
    melonloader = settings.setdefault("melonloader", {})
    if "policy" in melonloader:
        return melonloader_policy_value(melonloader.get("policy"))

    for key in ("post_download_policy", "win11_launch_policy"):
        value = melonloader_policy_value(melonloader.get(key))
        if value != "ask":
            return value
    return "ask"


def save_melonloader_policy(settings: dict, policy: object) -> str:
    selected = melonloader_policy_value(policy)
    melonloader = settings.setdefault("melonloader", {})
    melonloader["policy"] = selected
    melonloader.pop("post_download_policy", None)
    melonloader.pop("win11_launch_policy", None)
    save_settings(settings)
    return selected


def is_windows_11() -> bool:
    if os.name != "nt" or not hasattr(sys, "getwindowsversion"):
        return False
    try:
        return int(sys.getwindowsversion().build) >= 22000
    except Exception:
        return False


def desktop_dir() -> Path:
    home = Path(os.environ.get("USERPROFILE") or Path.home())
    return home / "Desktop"


def powershell_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def make_unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    counter = 2
    while True:
        candidate = path.with_name(f"{path.stem} ({counter}){path.suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


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
    exe_path = find_launch_executable(build.path)
    if exe_path is None:
        raise ShortcutError(f"No launchable .exe was found in {build.path}")
    desktop = desktop_dir()
    desktop.mkdir(parents=True, exist_ok=True)
    shortcut_path = desktop / f"{make_windows_safe(build.name)}.lnk"
    return create_windows_shortcut(
        shortcut_path,
        exe_path,
        exe_path.parent,
        f"Rec Room build: {build.name}",
        exe_path,
    )


def scan_local_builds(settings: dict) -> list[LocalBuild]:
    root = depot_root(settings)
    if not root.exists():
        return []

    builds: list[LocalBuild] = []
    manifests = settings.get("manifests", {})
    if not isinstance(manifests, dict):
        return []

    for manifest_id, record in manifests.items():
        if not isinstance(record, dict):
            continue
        path_value = record.get("path")
        if not isinstance(path_value, str) or not path_value.strip():
            continue
        child = Path(path_value)
        if not child.exists() or not child.is_dir() or not is_within_directory(child, root):
            continue
        try:
            modified_ts = child.stat().st_mtime
        except OSError:
            modified_ts = 0.0
        builds.append(
            LocalBuild(
                path=child,
                name=child.name,
                manifest_id=str(manifest_id),
                launcher=find_launcher_name(child),
                modified_ts=modified_ts,
                preview=False,
            )
        )

    builds.sort(key=lambda item: (-item.modified_ts, item.name.lower()))
    return builds


def render_home(settings: dict) -> None:
    builds = scan_local_builds(settings)
    Noir.header(len(builds), bool(settings.get("fake_mode", True)), depot_root(settings))
    Noir.section("Menu")
    Noir.menu(
        [
            ("1", "Download build"),
            ("2", "Local builds"),
            ("3", "Settings"),
            ("0", "Exit"),
        ]
    )
    Noir.line(color=Noir.DARK)


def status_checks(settings: dict) -> None:
    Noir.section("Checks")
    app_update = settings.get("app_update", {})
    app_status = str(app_update.get("status") or "unknown")
    latest = str(app_update.get("latest") or "?")
    if app_status == "outdated":
        Noir.step("GitHub", "UPDATE", f"latest v{latest}")
    elif app_status == "check_failed":
        Noir.step("GitHub", "WARN", "check failed")
    elif app_status == "dev":
        Noir.step("GitHub", "OK", f"dev / latest v{latest}")
    else:
        Noir.step("GitHub", "OK", f"v{latest}")
    Noir.step("Depot", "OK", str(DEPOT_ID))
    info = settings.get("depotdownloader", {})
    version = str(info.get("version") or "local")
    Noir.step("DepotDownloader", "OK", version)


def remember_manifest(settings: dict, bundle: ManifestBundle, build_dir: Path) -> None:
    manifests = settings.setdefault("manifests", {})
    record = {
        "path": str(build_dir),
        "updated_at": now_iso(),
    }
    if bundle.beta_branch:
        record["beta_branch"] = bundle.beta_branch
    manifests[bundle.manifest_id] = record
    append_unique_recent(settings, bundle.manifest_id)
    settings["last_manifest"] = bundle.manifest_id
    prune_remembered_manifests(settings)
    save_settings(settings)


def write_error_log(exc: BaseException) -> None:
    error_log_path().write_text(
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        encoding="utf-8",
        errors="replace",
    )


def download_build_workflow(settings: dict) -> None:
    Noir.clear()
    Noir.header(len(scan_local_builds(settings)), False, depot_root(settings))
    Noir.section("Download")
    raw_manifest_id = prompt("Manifest ID")
    if not raw_manifest_id:
        Noir.warn("No manifest entered.")
        press_enter()
        return
    manifest_id = normalize_manifest_id(raw_manifest_id)
    if manifest_id is None:
        Noir.warn("Manifest ID must be numbers only.")
        press_enter()
        return

    Noir.section("Lookup")
    bundle = lookup_manifest_bundle(manifest_id)
    build_dir = manifest_download_path(settings, bundle)
    Noir.kv("Date", bundle.date_label)
    if bundle.beta_branch:
        Noir.kv("Beta", bundle.beta_branch)
    patch_status = "invalid" if bundle.patch_error else bundle.patch_path or "none"
    Noir.kv("Patch", patch_status)
    Noir.kv("Folder", str(build_dir))

    existing_action = existing_manifest_menu(settings, bundle)
    if existing_action == "back":
        return
    if existing_action == "replace":
        try:
            replace_manifest_download(settings, bundle)
        except DownloadError as exc:
            Noir.err(str(exc))
            press_enter()
            return
        Noir.ok("Existing manifest folder removed.")

    credentials = ensure_credentials_for_download()
    if credentials is None:
        press_enter()
        return
    username, password = credentials

    exe_path = depotdownloader_exe_path()
    if not exe_path.exists():
        try:
            ensure_depotdownloader(settings)
        except DownloadError as exc:
            Noir.err(str(exc))
            press_enter()
            return
        if not exe_path.exists():
            Noir.err(f"DepotDownloader was not found: {exe_path}")
            press_enter()
            return

    build_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(exe_path),
        "-app", str(APP_ID),
        "-depot", str(DEPOT_ID),
        "-manifest", manifest_id,
    ]
    if bundle.beta_branch:
        cmd.extend(["-beta", bundle.beta_branch])
    cmd.extend(
        [
            "-dir", str(build_dir),
            "-username", username,
            "-password", password,
        ]
    )

    Noir.section("DepotDownloader")
    Noir.kv("Manifest", manifest_id)
    if bundle.beta_branch:
        Noir.kv("Beta", bundle.beta_branch)
    Noir.kv("Folder", str(build_dir))
    try:
        exit_code = stream_depotdownloader(cmd, display_root=build_dir)
    except DownloadError as exc:
        Noir.err(str(exc))
        Noir.kv("Path", str(exe_path))
        press_enter()
        return

    if exit_code != 0:
        Noir.section("Done")
        Noir.err(f"DepotDownloader exited with code {exit_code}.")
        Noir.kv("Log", str(log_path()))
        press_enter()
        return

    Noir.ok("Download finished.")
    normalize_beta_download_layout(build_dir, bundle)
    clean_build_metadata(build_dir)
    apply_patch_payload(build_dir, bundle)
    remember_manifest(settings, bundle, build_dir)
    downloaded_build = LocalBuild(
        path=build_dir,
        name=build_dir.name,
        manifest_id=manifest_id,
        launcher=find_launcher_name(build_dir),
        modified_ts=time.time(),
        preview=False,
    )
    prompt_melonloader_after_download(downloaded_build, settings)

    Noir.section("Done")
    Noir.ok(str(build_dir))
    Noir.kv("Log", str(log_path()))
    press_enter()


def print_builds(builds: list[LocalBuild]) -> None:
    Noir.section("Local Builds")
    if not builds:
        Noir.warn("No local builds were found.")
        return
    for index, build in enumerate(builds, start=1):
        tag = "local" if build.preview else "folder"
        print(
            Noir.c(Noir.ORANGE, f"{index:>2}. ")
            + Noir.c(Noir.WHITE, build.name)
            + Noir.c(Noir.GRAY, f"  {tag}")
        )
        Noir.kv("manifest", build.manifest_id)
        Noir.kv("launcher", build.launcher)
        Noir.kv("path", str(build.path))
        Noir.line(color=Noir.DARK)


def choose_build(builds: list[LocalBuild]) -> LocalBuild | None:
    if not builds:
        return None
    while True:
        raw = prompt("Build number or B", "b").lower()
        if raw in {"b", "back", "0"}:
            return None
        if raw.isdigit():
            index = int(raw)
            if 1 <= index <= len(builds):
                return builds[index - 1]
        Noir.warn("That build number is not valid.")


def open_path(path: Path) -> None:
    if os.name == "nt":
        os.startfile(str(path))
        return
    if sys.platform == "darwin":
        subprocess.Popen(["open", str(path)])
    else:
        subprocess.Popen(["xdg-open", str(path)])


def install_melonloader_for_build(build: LocalBuild, settings: dict) -> bool:
    try:
        install_melonloader_to_build(build.path, settings)
        return True
    except (DownloadError, error.HTTPError, error.URLError, TimeoutError, OSError) as exc:
        Noir.err(str(exc))
        return False


def prompt_melonloader_after_download(build: LocalBuild, settings: dict) -> None:
    if not is_historical_melonloader_build(build):
        return

    policy = current_melonloader_policy(settings)
    if policy == "always_install":
        install_melonloader_for_build(build, settings)
        return
    if policy == "never_install":
        return

    Noir.section("MelonLoader")
    Noir.warn("2016-2017 build downloaded.")
    Noir.info("Install MelonLoader 0.5.7 x64 into this build?")
    Noir.blue_info(MELONLOADER_PROMPT_INFO)
    Noir.menu(
        [
            ("1", "Skip"),
            ("2", "Install every time"),
            ("3", "Reject"),
            ("4", "Ask every time"),
        ]
    )
    Noir.line(color=Noir.DARK)
    choice = prompt_choice({"1", "2", "3", "4"})
    if choice == "1":
        return
    elif choice == "2":
        save_melonloader_policy(settings, "always_install")
        install_melonloader_for_build(build, settings)
    elif choice == "3":
        save_melonloader_policy(settings, "never_install")
        Noir.warn("MelonLoader rejected for future 2016-2017 builds.")
    elif choice == "4":
        save_melonloader_policy(settings, "ask")
        Noir.ok("MelonLoader will ask every time.")


def handle_windows11_historical_launch(build: LocalBuild, settings: dict) -> bool:
    if not is_windows_11() or not is_historical_melonloader_build(build):
        return True

    policy = current_melonloader_policy(settings)
    if policy == "always_install":
        return install_melonloader_for_build(build, settings)
    if policy == "never_install":
        return True

    Noir.section("Windows 11")
    Noir.warn("Windows 11 detected on a 2016-2017 build.")
    Noir.info("MelonLoader 0.5.7 x64 can be installed before launch.")
    Noir.blue_info(MELONLOADER_PROMPT_INFO)
    Noir.menu(
        [
            ("1", "Skip"),
            ("2", "Install every time"),
            ("3", "Reject"),
            ("4", "Ask every time"),
        ]
    )
    Noir.line(color=Noir.DARK)
    choice = prompt_choice({"1", "2", "3", "4"})
    if choice == "1":
        return True
    if choice == "2":
        save_melonloader_policy(settings, "always_install")
        return install_melonloader_for_build(build, settings)
    if choice == "3":
        save_melonloader_policy(settings, "never_install")
        Noir.warn("MelonLoader rejected for future 2016-2017 builds.")
        return True
    save_melonloader_policy(settings, "ask")
    Noir.ok("MelonLoader will ask every time.")
    return True


def launch_build(build: LocalBuild, settings: dict) -> None:
    exe_path = find_launch_executable(build.path)
    if exe_path is None:
        Noir.warn("No launchable .exe was found.")
        return
    if not handle_windows11_historical_launch(build, settings):
        Noir.warn("Launch canceled.")
        return
    open_path(exe_path)
    Noir.ok(f"Launched: {exe_path.name}")


def build_actions(build: LocalBuild, settings: dict) -> None:
    historical = is_historical_melonloader_build(build)
    while True:
        Noir.clear()
        Noir.header(1, build.preview, build.path.parent)
        Noir.section(build.name)
        Noir.kv("Path", str(build.path))
        Noir.kv("Manifest", build.manifest_id)
        Noir.kv("Launcher", build.launcher)
        rows = [
            ("1", "Open folder"),
            ("2", "Launch"),
        ]
        choices = {"1", "2", "0"}
        if historical:
            rows.append(("3", f"Install MelonLoader {MELONLOADER_RELEASE_TAG}"))
            choices.add("3")
        rows.append(("0", "Back"))
        Noir.menu(rows)
        Noir.line(color=Noir.DARK)
        choice = prompt_choice(choices)
        if choice == "0":
            return
        if choice == "1":
            open_path(build.path)
            Noir.ok("Build folder opened.")
            press_enter()
        elif choice == "2":
            launch_build(build, settings)
            press_enter()
        elif choice == "3":
            install_melonloader_for_build(build, settings)
            press_enter()


def browse_local_builds(settings: dict) -> None:
    while True:
        Noir.clear()
        builds = scan_local_builds(settings)
        Noir.header(len(builds), bool(settings.get("fake_mode", True)), depot_root(settings))
        print_builds(builds)
        if not builds:
            press_enter()
            return
        build = choose_build(builds)
        if build is None:
            return
        build_actions(build, settings)


def create_shortcut_from_menu(settings: dict) -> None:
    Noir.clear()
    builds = scan_local_builds(settings)
    Noir.header(len(builds), False, depot_root(settings))
    print_builds(builds)
    build = choose_build(builds)
    if build is None:
        return
    try:
        shortcut = create_build_shortcut(build)
    except ShortcutError as exc:
        Noir.err(str(exc))
    else:
        Noir.ok(f"Desktop shortcut created: {shortcut}")
    press_enter()


def melonloader_settings(settings: dict) -> None:
    Noir.clear()
    Noir.header(len(scan_local_builds(settings)), False, depot_root(settings))
    Noir.section("MelonLoader")
    Noir.kv("Current", melonloader_policy_label(current_melonloader_policy(settings)))
    Noir.menu(
        [
            ("1", "Skip"),
            ("2", "Install every time"),
            ("3", "Reject"),
            ("4", "Ask every time"),
        ]
    )
    Noir.line(color=Noir.DARK)
    choice = prompt_choice({"1", "2", "3", "4"})
    if choice == "1":
        return
    selected = {
        "2": "always_install",
        "3": "never_install",
        "4": "ask",
    }[choice]
    save_melonloader_policy(settings, selected)
    Noir.ok(f"MelonLoader: {melonloader_policy_label(selected)}")
    press_enter()


def open_build_storage(settings: dict) -> None:
    root = depot_root(settings)
    root.mkdir(parents=True, exist_ok=True)
    open_path(root)
    Noir.ok(f"Opened build storage: {root}")
    press_enter()


def steam_settings(settings: dict) -> None:
    while True:
        Noir.clear()
        Noir.header(len(scan_local_builds(settings)), False, depot_root(settings))
        Noir.section("Steam")
        try:
            state = load_credential_state()
        except CredentialError as exc:
            Noir.warn(str(exc))
            state = {"version": 1, "reuse_credentials": True}

        saved = has_saved_credentials(state)
        reuse = bool(state.get("reuse_credentials", True))
        if saved:
            Noir.ok("Saved login found.")
            Noir.kv("Reuse", "on" if reuse else "off")
        else:
            Noir.warn("No saved login.")
        Noir.menu(
            [
                ("1", "Replace login" if saved else "Set login"),
                ("2", "Enable reuse" if not reuse else "Disable reuse"),
                ("3", "Clear login"),
                ("0", "Back"),
            ]
        )
        Noir.line(color=Noir.DARK)
        choice = prompt_choice({"1", "2", "3", "0"})
        if choice == "0":
            return
        if choice == "1":
            try:
                prompt_for_credentials()
                Noir.ok("Login saved.")
            except CredentialError as exc:
                Noir.err(str(exc))
            press_enter()
        elif choice == "2":
            if not saved:
                Noir.warn("Save a login first.")
            else:
                state["reuse_credentials"] = not reuse
                save_credential_state(state)
                Noir.ok(f"Reuse {'enabled' if state['reuse_credentials'] else 'disabled'}.")
            press_enter()
        elif choice == "3":
            if credentials_path().exists():
                credentials_path().unlink()
            Noir.ok("Login cleared.")
            press_enter()


def preview_settings(settings: dict) -> None:
    while True:
        Noir.clear()
        Noir.header(len(scan_local_builds(settings)), bool(settings.get("fake_mode", True)), depot_root(settings))
        Noir.section("Settings")
        Noir.menu(
            [
                ("1", "Steam"),
                ("2", "Open storage"),
                ("3", "Shortcut"),
                ("4", "Status"),
                ("5", "MelonLoader"),
                ("6", "Raw settings"),
                ("0", "Back"),
            ]
        )
        Noir.line(color=Noir.DARK)
        choice = prompt_choice({"1", "2", "3", "4", "5", "6", "0"})
        if choice == "0":
            return
        if choice == "1":
            steam_settings(settings)
        elif choice == "2":
            open_build_storage(settings)
        elif choice == "3":
            create_shortcut_from_menu(settings)
        elif choice == "4":
            system_check(settings)
        elif choice == "5":
            melonloader_settings(settings)
        elif choice == "6":
            print(json.dumps(settings, indent=2))
            press_enter()


def system_check(settings: dict) -> None:
    Noir.clear()
    Noir.header(len(scan_local_builds(settings)), bool(settings.get("fake_mode", True)), depot_root(settings))
    status_checks(settings)
    Noir.section("Files")
    Noir.kv("Settings", str(settings_path()))
    Noir.kv("Storage", str(depot_root(settings)))
    press_enter()


def main() -> int:
    Noir.configure()
    settings = load_settings()
    settings["last_launch"] = now_iso()
    settings["fake_mode"] = False
    save_settings(settings)

    update_exit_code = check_app_release(settings, enforce=bool(getattr(sys, "frozen", False)))
    if update_exit_code != 0:
        return update_exit_code
    settings = load_settings()

    try:
        ensure_depotdownloader(settings)
    except DownloadError as exc:
        Noir.err(str(exc))
        return 1
    settings = load_settings()

    while True:
        Noir.clear()
        render_home(settings)
        choice = prompt_choice({"1", "2", "3", "0"})
        if choice == "0":
            Noir.ok("Bye.")
            return 0
        try:
            if choice == "1":
                download_build_workflow(settings)
            elif choice == "2":
                browse_local_builds(settings)
            elif choice == "3":
                preview_settings(settings)
        except Exception as exc:
            write_error_log(exc)
            Noir.err(str(exc))
            Noir.kv("Log", str(error_log_path()))
            press_enter()
        settings = load_settings()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print()
        Noir.warn("Cancelled.")
        raise SystemExit(130)
