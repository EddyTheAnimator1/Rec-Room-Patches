import json
import os
import re
import shutil
import subprocess
import ssl
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
LOG_NAME = "last_recagain_noir.log"
ERROR_LOG_NAME = "last_release_noir_error.log"
PREVIEW_STATE_VERSION = 1
USER_AGENT = "Release-Noir/1.0"
STEAMDB_CSV_NAME = "steamdb.csv"
RECAGAIN_DOWNLOAD_URL = "https://archive.recagain.site/download/{kind}/{identifier}"
RECAGAIN_HEALTHCHECK_URL = "https://archive.recagain.site/"
RECAGAIN_BUILDING_MESSAGE = "You must wait for this build, it's currently downloading on the recagain servers"
MELONLOADER_RELEASE_TAG = "v0.5.7"
MELONLOADER_ASSET_NAME = "MelonLoader.x64.zip"
MELONLOADER_PROMPT_INFO = (
    "MelonLoader ensures compatibility, for some. The builds might crash unless you have "
    "MelonLoader. For others, it works normally WITHOUT MelonLoader. This is recommended "
    "to be first TURNED off. Though, if you crash right away with no trace, you should "
    "INSTALL MelonLoader."
)
MELONLOADER_SETTINGS_TUTORIAL = "Main menu > Settings > MelonLoader."
STEAM_OWNERSHIP_SETTING = "owns_rec_room_on_steam"
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


class RecagainBuildingError(DownloadError):
    pass


class ManifestError(RuntimeError):
    pass


class PatchError(RuntimeError):
    pass


class ShortcutError(RuntimeError):
    pass


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
        title_part = cls.c(cls.BOLD + cls.ORANGE, title)
        print(title_part)
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
    def red_inf(cls, text: str) -> None:
        print(f"{cls.chip('INF', cls.RED)} {cls.c(cls.RED, text)}")

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
        normalized_result = result.strip().upper()
        if normalized_result in {"ERROR", "ERR", "FAILED", "FAIL"}:
            result_color = cls.RED
        elif normalized_result in {"WARN", "WARNING", "UPDATE"}:
            result_color = cls.GOLD
        else:
            result_color = cls.GREEN
        print(
            cls.c(cls.GRAY, f"  {label} {dots} ")
            + cls.c(result_color, result)
            + (cls.c(cls.DIM + cls.GRAY, f"  {detail}") if detail else "")
        )


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def script_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def bundled_resource_path(name: str) -> Path | None:
    bundle_dir = getattr(sys, "_MEIPASS", None)
    if not bundle_dir:
        return None
    candidate = Path(bundle_dir) / name
    return candidate if candidate.exists() and candidate.is_file() else None


def local_data_path(name: str) -> Path:
    return script_dir() / name


def settings_path() -> Path:
    return script_dir() / REMEMBERING_NAME


def log_path() -> Path:
    return script_dir() / LOG_NAME


def error_log_path() -> Path:
    return script_dir() / ERROR_LOG_NAME


def default_storage_root() -> Path:
    return script_dir() / "Builds"


def legacy_default_storage_root() -> Path:
    return script_dir() / "depots" / str(DEPOT_ID)


def is_legacy_default_storage_root(path: Path) -> bool:
    try:
        return path.resolve() == legacy_default_storage_root().resolve()
    except OSError:
        return str(path).lower() == str(legacy_default_storage_root()).lower()


def depot_root(settings: dict | None = None) -> Path:
    root = (settings or {}).get("storage_root")
    if root:
        candidate = Path(root)
        if not is_legacy_default_storage_root(candidate):
            return candidate
    return default_storage_root()


def preview_receipt_dir() -> Path:
    return script_dir() / ".release_noir"


def default_settings() -> dict:
    return {
        "state_version": PREVIEW_STATE_VERSION,
        "fake_mode": False,
        "theme": "orange-black",
        STEAM_OWNERSHIP_SETTING: None,
        "created_at": now_iso(),
        "last_launch": None,
        "storage_root": str(default_storage_root()),
        "app_update": {},
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
    if is_legacy_default_storage_root(Path(str(settings.get("storage_root") or ""))):
        settings["storage_root"] = str(default_storage_root())
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


def owns_rec_room_on_steam(settings: dict) -> bool:
    return settings.get(STEAM_OWNERSHIP_SETTING) is True


def steam_ownership_label(settings: dict) -> str:
    value = settings.get(STEAM_OWNERSHIP_SETTING)
    if value is True:
        return "Yes"
    if value is False:
        return "No"
    return "Not answered"


def ask_steam_ownership(settings: dict, *, first_launch: bool) -> bool:
    Noir.clear()
    Noir.section("Very Important")
    if not first_launch:
        Noir.kv("Current", steam_ownership_label(settings))
    Noir.warn(
        "Do you own Rec Room on steam? "
        "(If yes, this will SIGNIFICANTLY reduce compatibility issues.)"
    )
    if first_launch:
        Noir.info(
            "For this to work properly. Ensure you own SteamVR on your account. "
            "(E.g. valid license)"
        )
    Noir.menu(
        [
            ("1", "Yes"),
            ("2", "No"),
        ]
    )
    Noir.line(color=Noir.DARK)
    choice = prompt_choice({"1", "2"})
    selected = choice == "1"
    settings[STEAM_OWNERSHIP_SETTING] = selected
    save_settings(settings)
    Noir.ok(f"Owns Rec Room on Steam: {'Yes' if selected else 'No'}")
    return selected


def ensure_steam_ownership_answered(settings: dict) -> None:
    if isinstance(settings.get(STEAM_OWNERSHIP_SETTING), bool):
        return
    ask_steam_ownership(settings, first_launch=True)


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


def create_ssl_context(verify: bool = True) -> ssl.SSLContext:
    if verify:
        try:
            return ssl.create_default_context()
        except Exception:
            pass
    context = ssl._create_unverified_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def safe_urlopen(req: request.Request | str, timeout: int = 60):
    try:
        return request.urlopen(req, timeout=timeout, context=create_ssl_context(verify=True))
    except (error.URLError, ssl.SSLError) as exc:
        if isinstance(exc, error.HTTPError):
            raise
        try:
            return request.urlopen(req, timeout=timeout, context=create_ssl_context(verify=False))
        except Exception:
            raise exc


def request_json(url: str) -> dict:
    req = request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": USER_AGENT,
        },
    )
    with safe_urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
    if not isinstance(data, dict):
        raise DownloadError("GitHub returned an unexpected response.")
    return data


def request_bytes(url: str) -> bytes:
    req = request.Request(url, headers={"User-Agent": USER_AGENT})
    with safe_urlopen(req, timeout=60) as resp:
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


def ensure_repo_data_file(name: str) -> Path:
    local_path = local_data_path(name)
    if local_path.exists() and local_path.is_file():
        return local_path

    bundled = bundled_resource_path(name)
    if bundled is not None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(bundled, local_path)
        return local_path

    last_error: Exception | None = None
    for branch in PATCH_BRANCHES:
        try:
            data = fetch_repo_bytes(branch, name)
        except error.HTTPError as exc:
            last_error = exc
            if exc.code == 404:
                continue
        except (error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
        else:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(data)
            return local_path

    detail = f": {last_error}" if last_error else ""
    raise DownloadError(f"{name} was not found next to RecRoomPatches.exe and could not be downloaded from the repo{detail}")


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


def melonloader_zip_path() -> Path:
    local_path = local_data_path(MELONLOADER_ASSET_NAME)
    if local_path.exists() and local_path.is_file():
        return local_path

    bundled = bundled_resource_path(MELONLOADER_ASSET_NAME)
    if bundled is not None:
        return bundled

    if getattr(sys, "frozen", False):
        raise DownloadError(
            f"This RecRoomPatches.exe was built without bundled {MELONLOADER_ASSET_NAME}. "
            "Run the build-and-stage-release workflow again after the bundled MelonLoader step."
        )

    raise DownloadError(
        f"{MELONLOADER_ASSET_NAME} was not found next to Release.py. "
        "Source runs need the local zip; the release exe bundles it automatically."
    )


def terminal_columns() -> int:
    return max(60, shutil.get_terminal_size((Noir.width, 20)).columns)


def render_one_line(text: str, last_len: int = 0) -> int:
    width = terminal_columns()
    clean = " ".join(text.replace("\t", " ").split())
    if len(clean) > width - 1:
        clean = clean[: max(1, width - 4)] + "..."
    padding = " " * max(0, last_len - len(clean))
    sys.stdout.write("\r" + clean + padding)
    sys.stdout.flush()
    return len(clean)


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


def recagain_download_url(kind: str, identifier: str) -> str:
    return RECAGAIN_DOWNLOAD_URL.format(
        kind=parse.quote(kind, safe=""),
        identifier=parse.quote(identifier, safe=""),
    )


def recagain_state_from_json(data: bytes) -> dict | None:
    try:
        payload = json.loads(data.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def raise_for_recagain_state(data: bytes) -> None:
    payload = recagain_state_from_json(data)
    if payload is None:
        return

    state = str(payload.get("state") or "").strip().lower()
    if state == "building":
        raise RecagainBuildingError(RECAGAIN_BUILDING_MESSAGE)

    error_text = payload.get("error")
    if error_text:
        raise DownloadError(f"RecAgain failed: {error_text}")
    if state:
        raise DownloadError(f"RecAgain returned state: {state}")
    raise DownloadError("RecAgain returned JSON instead of a zip.")


def download_recagain_zip(bundle: ManifestBundle, work_dir: Path) -> Path:
    zip_path = work_dir / f"{bundle.manifest_id}.zip"
    url = recagain_download_url("manifest", bundle.manifest_id)
    req = request.Request(
        url,
        headers={
            "Accept": "application/zip, application/octet-stream, application/json;q=0.9",
            "User-Agent": USER_AGENT,
        },
    )

    last_len = 0
    spinner_index = 0
    try:
        resp_context = safe_urlopen(req, timeout=60)
    except error.HTTPError as exc:
        body = exc.read()
        if body:
            raise_for_recagain_state(body)
        raise DownloadError(f"RecAgain returned HTTP {exc.code}.") from exc

    with resp_context as resp:
        content_type = str(resp.headers.get("Content-Type") or "").lower()
        if "json" in content_type:
            raise_for_recagain_state(resp.read())

        total_header = resp.headers.get("Content-Length")
        total = int(total_header) if total_header and total_header.isdigit() else 0
        first_chunk = resp.read(DOWNLOAD_CHUNK)
        if first_chunk.lstrip().startswith(b"{"):
            raise_for_recagain_state(first_chunk + resp.read())

        read = len(first_chunk)
        with zip_path.open("wb") as handle:
            if first_chunk:
                handle.write(first_chunk)
            while True:
                chunk = resp.read(DOWNLOAD_CHUNK)
                if not chunk:
                    break
                handle.write(chunk)
                read += len(chunk)
                spinner_index = (spinner_index + 1) % len(SPINNER)
                spin = Noir.c(Noir.ORANGE, SPINNER[spinner_index])
                if total:
                    percent = (read / total) * 100
                    line = f"{spin} {percent:6.2f}% {zip_path.name}"
                else:
                    line = f"{spin} {read // 1024:>7} KB {zip_path.name}"
                last_len = render_one_line(line, last_len)

    sys.stdout.write("\n")
    if not zip_path.exists() or zip_path.stat().st_size == 0:
        raise DownloadError("RecAgain returned an empty zip.")
    return zip_path


def extracted_build_items(extract_dir: Path) -> list[Path]:
    return [
        item
        for item in extract_dir.iterdir()
        if item.name not in {"__MACOSX"} and not item.name.startswith(".DS_Store")
    ]


def extract_recagain_zip(zip_path: Path, build_dir: Path) -> None:
    extract_dir = zip_path.parent / "extract"
    staging_dir = zip_path.parent / "staged"
    clean_work_dir(extract_dir)
    clean_work_dir(staging_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)
    try:
        safe_extract_zip(zip_path, extract_dir)
    except zipfile.BadZipFile as exc:
        raise DownloadError("RecAgain archive was not a valid zip.") from exc

    items = extracted_build_items(extract_dir)
    if not items:
        raise DownloadError("RecAgain archive did not contain a build folder.")
    if build_dir.exists():
        raise DownloadError(f"Build folder already exists: {build_dir}")

    build_dir.parent.mkdir(parents=True, exist_ok=True)
    if len(items) == 1 and items[0].is_dir():
        shutil.move(str(items[0]), str(build_dir))
        return

    staging_dir.mkdir(parents=True, exist_ok=True)
    for item in items:
        shutil.move(str(item), str(staging_dir / item.name))
    shutil.move(str(staging_dir), str(build_dir))


def download_recagain_archive(settings: dict, bundle: ManifestBundle, build_dir: Path, *, replace_existing: bool) -> None:
    work_dir = build_dir.parent / f".recagain_{bundle.manifest_id}_{os.getpid()}"
    clean_work_dir(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    try:
        zip_path = download_recagain_zip(bundle, work_dir)
        if replace_existing:
            replace_manifest_download(settings, bundle)
        elif build_dir.exists():
            raise DownloadError(f"Build folder already exists: {build_dir}")
        extract_recagain_zip(zip_path, build_dir)
    finally:
        clean_work_dir(work_dir)


def melonloader_is_installed(build_dir: Path) -> bool:
    return (
        (build_dir / "version.dll").is_file()
        and (build_dir / "MelonLoader" / "MelonLoader.dll").is_file()
    )


def install_melonloader_to_build(build_dir: Path, settings: dict | None = None) -> None:
    if not build_dir.exists() or not build_dir.is_dir():
        raise DownloadError(f"Build folder was not found: {build_dir}")

    Noir.section("MelonLoader")
    if melonloader_is_installed(build_dir):
        Noir.ok(f"MelonLoader {MELONLOADER_RELEASE_TAG} already installed in {build_dir}")
        return

    zip_path = melonloader_zip_path()
    try:
        safe_extract_zip(zip_path, build_dir)
    except zipfile.BadZipFile as exc:
        raise DownloadError("MelonLoader archive was invalid.") from exc

    if settings is not None:
        settings.setdefault("melonloader", {}).update(
            {
                "version": MELONLOADER_RELEASE_TAG,
                "asset": MELONLOADER_ASSET_NAME,
                "source": str(zip_path),
                "installed_at": now_iso(),
                "last_build": str(build_dir),
            }
        )
        save_settings(settings)
    Noir.ok(f"MelonLoader {MELONLOADER_RELEASE_TAG} installed to {build_dir}")


def remove_melonloader_from_build(build_dir: Path, settings: dict | None = None) -> None:
    if not build_dir.exists() or not build_dir.is_dir():
        raise DownloadError(f"Build folder was not found: {build_dir}")

    Noir.section("MelonLoader")
    version_dll = build_dir / "version.dll"
    melonloader_dir = build_dir / "MelonLoader"
    removed = False

    if version_dll.exists():
        if not version_dll.is_file():
            raise DownloadError(f"MelonLoader path is not a file: {version_dll}")
        version_dll.unlink()
        removed = True

    if melonloader_dir.exists():
        if not melonloader_dir.is_dir():
            raise DownloadError(f"MelonLoader path is not a folder: {melonloader_dir}")
        shutil.rmtree(melonloader_dir)
        removed = True

    if not removed:
        Noir.warn(f"MelonLoader is not installed in {build_dir}")
        return

    if settings is not None:
        melonloader = settings.setdefault("melonloader", {})
        melonloader["removed_at"] = now_iso()
        melonloader["last_removed_build"] = str(build_dir)
        if melonloader.get("last_build") == str(build_dir):
            melonloader.pop("last_build", None)
        save_settings(settings)

    Noir.ok(f"MelonLoader removed from {build_dir}")


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


def effective_text_patch_content(instruction: dict, settings: dict) -> str:
    content = instruction["content"]
    target_name = Path(instruction["file"]).name.casefold()
    if (
        owns_rec_room_on_steam(settings)
        and target_name == "steam_appid.txt"
        and content == "250820"
    ):
        return str(APP_ID)
    return content


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


def apply_patch_payload(build_dir: Path, bundle: ManifestBundle, settings: dict) -> list[PatchResult]:
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
            content = effective_text_patch_content(instruction, settings)
            write_text_file(target, content, instruction["encoding"], instruction["overwrite"])
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


def find_mode_launchers(build_dir: Path) -> list[tuple[str, Path]]:
    launchers = []
    for filename, label in (
        ("screenmode.bat", "Screen Mode"),
        ("vr.bat", "VR"),
    ):
        path = build_dir / filename
        if path.is_file():
            launchers.append((label, path))
    return launchers


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
        return "Always reject"
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


def ping_recagain_server(timeout: int = 12) -> tuple[bool, str]:
    req = request.Request(
        RECAGAIN_HEALTHCHECK_URL,
        headers={"User-Agent": USER_AGENT},
        method="HEAD",
    )
    started = time.monotonic()
    try:
        with safe_urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 200) or 200)
    except error.HTTPError as exc:
        status = int(exc.code)
        if status >= 500:
            return False, f"HTTP {status}"
    except (error.URLError, ssl.SSLError, TimeoutError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        return False, str(reason)

    elapsed_ms = max(1, round((time.monotonic() - started) * 1000))
    return True, f"HTTP {status} / {elapsed_ms} ms"


def check_recagain_with_progress(timeout: int = 12) -> tuple[bool, str]:
    result: dict[str, object] = {}

    def worker() -> None:
        try:
            ok, detail = ping_recagain_server(timeout=timeout)
        except Exception as exc:
            ok, detail = False, str(exc)
        result["ok"] = ok
        result["detail"] = detail

    import threading

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    label = "RecAgain"
    dots = "." * max(1, 24 - len(label))
    prefix = Noir.c(Noir.GRAY, f"  {label} {dots} ")
    spinner_index = 0

    while thread.is_alive():
        spinner = Noir.c(Noir.ORANGE, SPINNER[spinner_index])
        sys.stdout.write("\r" + prefix + spinner)
        sys.stdout.flush()
        spinner_index = (spinner_index + 1) % len(SPINNER)
        thread.join(0.08)

    thread.join()
    ok = bool(result.get("ok"))
    detail = str(result.get("detail") or "Unknown response")
    status = "OK" if ok else "ERROR"
    status_color = Noir.GREEN if ok else Noir.RED
    final_line = (
        prefix
        + Noir.c(status_color, status)
        + Noir.c(Noir.DIM + Noir.GRAY, f"  {detail}")
    )
    sys.stdout.write("\r" + final_line + "\n")
    sys.stdout.flush()
    return ok, detail


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
    Noir.step("Storage", "OK", "Builds")
    check_recagain_with_progress()
    steamdb_path = local_data_path(STEAMDB_CSV_NAME)
    Noir.step("SteamDB CSV", "OK" if steamdb_path.exists() else "WARN", steamdb_path.name)


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
    try:
        bundle = lookup_manifest_bundle(manifest_id)
    except ManifestError as exc:
        Noir.err(str(exc))
        press_enter()
        return
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
    replace_existing = existing_action == "replace"

    Noir.section("RecAgain")
    Noir.kv("Manifest", manifest_id)
    Noir.kv("Folder", str(build_dir))
    try:
        download_recagain_archive(settings, bundle, build_dir, replace_existing=replace_existing)
    except RecagainBuildingError as exc:
        Noir.warn(str(exc))
        press_enter()
        return
    except DownloadError as exc:
        Noir.err(str(exc))
        press_enter()
        return

    Noir.ok("Download finished.")
    normalize_beta_download_layout(build_dir, bundle)
    clean_build_metadata(build_dir)
    apply_patch_payload(build_dir, bundle, settings)
    remember_manifest(settings, bundle, build_dir)
    log_path().write_text(
        "\n".join(
            [
                f"manifest={manifest_id}",
                f"download_url={recagain_download_url('manifest', manifest_id)}",
                f"date_label={bundle.date_label}",
                f"path={build_dir}",
                f"completed_at={now_iso()}",
            ]
        ),
        encoding="utf-8",
        errors="replace",
    )
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


def remove_melonloader_for_build(build: LocalBuild, settings: dict) -> bool:
    try:
        remove_melonloader_from_build(build.path, settings)
        return True
    except (DownloadError, OSError) as exc:
        Noir.err(str(exc))
        return False


def print_melonloader_guidance(settings: dict, *, show_settings_tutorial: bool = True) -> None:
    if not owns_rec_room_on_steam(settings):
        Noir.red_inf(
            "[BIG WARNING]: MelonLoader is NOT recommended for users that do NOT own "
            "Rec Room on their Steam account. It might cause the V:0000065432 warning."
        )
    Noir.blue_info(MELONLOADER_PROMPT_INFO)
    if show_settings_tutorial:
        Noir.info(f"MelonLoader settings: {MELONLOADER_SETTINGS_TUTORIAL}")


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
    print_melonloader_guidance(settings)
    Noir.warn("2016-2017 build downloaded.")
    Noir.info("Install MelonLoader 0.5.7 x64 into this build?")
    Noir.menu(
        [
            ("1", "Install only this time"),
            ("2", "Install every time"),
            ("3", "Reject only this time"),
            ("4", "Always reject"),
        ]
    )
    Noir.line(color=Noir.DARK)
    choice = prompt_choice({"1", "2", "3", "4"})
    if choice == "1":
        install_melonloader_for_build(build, settings)
    elif choice == "2":
        save_melonloader_policy(settings, "always_install")
        install_melonloader_for_build(build, settings)
    elif choice == "3":
        Noir.warn("MelonLoader rejected for this build only.")
    elif choice == "4":
        save_melonloader_policy(settings, "never_install")
        Noir.warn("MelonLoader will always be rejected for future 2016-2017 builds.")


def handle_windows11_historical_launch(build: LocalBuild, settings: dict) -> bool:
    if not is_windows_11() or not is_historical_melonloader_build(build):
        return True

    policy = current_melonloader_policy(settings)
    if policy == "always_install":
        return install_melonloader_for_build(build, settings)
    if policy == "never_install":
        return True

    Noir.section("Windows 11")
    print_melonloader_guidance(settings)
    Noir.warn("Windows 11 detected on a 2016-2017 build.")
    Noir.info("MelonLoader 0.5.7 x64 can be installed before launch.")
    Noir.menu(
        [
            ("1", "Install only this time"),
            ("2", "Install every time"),
            ("3", "Reject only this time"),
            ("4", "Always reject"),
        ]
    )
    Noir.line(color=Noir.DARK)
    choice = prompt_choice({"1", "2", "3", "4"})
    if choice == "1":
        return install_melonloader_for_build(build, settings)
    if choice == "2":
        save_melonloader_policy(settings, "always_install")
        return install_melonloader_for_build(build, settings)
    if choice == "3":
        Noir.warn("MelonLoader rejected for this launch only.")
        return True
    save_melonloader_policy(settings, "never_install")
    Noir.warn("MelonLoader will always be rejected for future 2016-2017 builds.")
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


def launch_build_mode(
    build: LocalBuild,
    settings: dict,
    label: str,
    launcher_path: Path,
) -> None:
    if not handle_windows11_historical_launch(build, settings):
        Noir.warn("Launch canceled.")
        return
    if os.name == "nt":
        subprocess.Popen(
            ["cmd.exe", "/d", "/c", "call", launcher_path.name],
            cwd=str(launcher_path.parent),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    else:
        open_path(launcher_path)
    Noir.ok(f"Launched {label}: {launcher_path.name}")


def build_actions(build: LocalBuild, settings: dict) -> None:
    historical = is_historical_melonloader_build(build)
    while True:
        Noir.clear()
        Noir.header(1, build.preview, build.path.parent)
        Noir.section(build.name)
        Noir.kv("Path", str(build.path))
        Noir.kv("Manifest", build.manifest_id)
        Noir.kv("Launcher", build.launcher)
        rows = [("1", "Open folder")]
        choices = {"1", "0"}
        launch_actions: dict[str, tuple[str, Path] | None] = {}
        mode_launchers = find_mode_launchers(build.path)
        if mode_launchers:
            for label, launcher_path in mode_launchers:
                action = str(len(rows) + 1)
                rows.append((action, label))
                choices.add(action)
                launch_actions[action] = (label, launcher_path)
        else:
            rows.append(("2", "Launch"))
            choices.add("2")
            launch_actions["2"] = None
        melonloader_installed = melonloader_is_installed(build.path)
        melonloader_choice = ""
        if historical:
            melonloader_action = (
                "Remove MelonLoader"
                if melonloader_installed
                else f"Install MelonLoader {MELONLOADER_RELEASE_TAG}"
            )
            melonloader_choice = str(len(rows) + 1)
            rows.append((melonloader_choice, melonloader_action))
            choices.add(melonloader_choice)
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
        elif choice in launch_actions:
            mode_launcher = launch_actions[choice]
            if mode_launcher is None:
                launch_build(build, settings)
            else:
                label, launcher_path = mode_launcher
                launch_build_mode(build, settings, label, launcher_path)
            press_enter()
        elif choice == melonloader_choice:
            if melonloader_installed:
                remove_melonloader_for_build(build, settings)
            else:
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
    print_melonloader_guidance(settings, show_settings_tutorial=False)
    Noir.kv("Current", melonloader_policy_label(current_melonloader_policy(settings)))
    Noir.menu(
        [
            ("1", "Back"),
            ("2", "Install every time"),
            ("3", "Always reject"),
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


def steam_ownership_settings(settings: dict) -> None:
    ask_steam_ownership(settings, first_launch=False)
    press_enter()


def open_build_storage(settings: dict) -> None:
    root = depot_root(settings)
    root.mkdir(parents=True, exist_ok=True)
    open_path(root)
    Noir.ok(f"Opened build storage: {root}")
    press_enter()


def preview_settings(settings: dict) -> None:
    while True:
        Noir.clear()
        Noir.header(len(scan_local_builds(settings)), bool(settings.get("fake_mode", True)), depot_root(settings))
        Noir.section("Settings")
        Noir.menu(
            [
                ("1", "Open storage"),
                ("2", "Shortcut"),
                ("3", "Status"),
                ("4", "Steam ownership"),
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
            open_build_storage(settings)
        elif choice == "2":
            create_shortcut_from_menu(settings)
        elif choice == "3":
            system_check(settings)
        elif choice == "4":
            steam_ownership_settings(settings)
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
    ensure_steam_ownership_answered(settings)
    settings["last_launch"] = now_iso()
    settings["fake_mode"] = False
    save_settings(settings)

    update_exit_code = check_app_release(settings, enforce=bool(getattr(sys, "frozen", False)))
    if update_exit_code != 0:
        return update_exit_code
    settings = load_settings()

    try:
        ensure_repo_data_file(STEAMDB_CSV_NAME)
    except DownloadError as exc:
        Noir.warn(str(exc))
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
