from __future__ import annotations

import asyncio
import hmac
import hashlib
import importlib.util
import json
import math
import mimetypes
import os
import re
import secrets
import socket
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import defaultdict, deque
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable, Iterator

from fastapi import Depends, FastAPI, HTTPException, Request, Security, WebSocket, WebSocketException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field
from starlette import status

from content_filter import (
    ContentFilter,
    ContentFilterError,
    FilterResult,
    ProhibitedProfileText,
    environment_allowed_words,
    environment_enabled,
)
import moderation_service
import redis_state
import timed_content


API_VERSION_RE = re.compile(r"^[A-Za-z0-9_]+$")
API_VERSION_ALIASES: dict[str, str] = {}
IMAGE_DATA_DIR_NAME = "IMAGES"
VIDEO_DATA_DIR_NAME = "Videos"
BACKEND_IMAGE_DIR_NAME = "RR"
PLAYER_IMAGE_DIR_NAME = "RRPlayer"
IMAGE_BUCKET_NAMES = {BACKEND_IMAGE_DIR_NAME, PLAYER_IMAGE_DIR_NAME}
ALLOWED_DATA_ROOT_EXTENSIONS = {".json"}
ALLOWED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg"}
ALLOWED_VIDEO_EXTENSIONS = {".mov", ".mp4", ".m4v"}
SQLITE_SIDECAR_RE = re.compile(r"^database\.sqlite3(?:-(?:journal|wal|shm))?$")
DEFAULT_LOCAL_PORT = 7979
DEFAULT_RAILWAY_PUBLIC_BASE_URL = "https://brand-new-all-production.up.railway.app"
DEFAULT_RAILWAY_PUBLIC_WEBSOCKET_URL = "ws://thomas.proxy.rlwy.net:44698"
PUBLIC_BASE_URL_ENV_NAMES = (
    "RECROOM_PUBLIC_BASE_URL",
    "RECROOM_API_PUBLIC_BASE_URL",
    "RECROOM_SERVER_PUBLIC_BASE_URL",
)
PUBLIC_WEBSOCKET_URL_ENV_NAMES = (
    "RECROOM_PUBLIC_WEBSOCKET_URL",
    "RECROOM_PUBLIC_WS_URL",
    "RECROOM_WEBSOCKET_PUBLIC_URL",
)
TLS_CERTFILE_ENV_NAME = "RECROOM_TLS_CERTFILE"
TLS_KEYFILE_ENV_NAME = "RECROOM_TLS_KEYFILE"
DISABLE_LOCAL_TLS_ENV_NAME = "RECROOM_DISABLE_LOCAL_TLS"
DEFAULT_LOCAL_TLS_CERTFILE = "20january2022-localhost.pem"
DEFAULT_LOCAL_TLS_KEYFILE = "20january2022-localhost.key"
LOCAL_TRANSPORT_POLICY_FILE = "transport_policy.json"
DEFAULT_LOCAL_HTTPS_FROM_DATE = "2022-01-01"
DEFAULT_MAX_REQUEST_BODY_BYTES = 8 * 1024 * 1024
DEFAULT_CREATED_PLAYER_EMAIL = "idontwanttoguess@gmail.com"
DEV_PERMISSIONS = ["DEV"]
COACH_PLAYER_ID = "00000000-0000-0000-0000-000000000099"
ADMIN_KEY_ENV_NAMES = ("RECROOM_ADMIN_SECRET", "RECROOM_ADMIN_BAN_KEY", "RECROOM_ADMIN_KEY", "RR_ADMIN_KEY")
ADMIN_SESSION_COOKIE_NAME = "recroom_admin_session"
DEFAULT_ADMIN_SESSION_TTL_SECONDS = 8 * 60 * 60
ADMIN_SESSION_TOUCH_INTERVAL_SECONDS = 5 * 60
ADMIN_CSRF_HEADER = "x-recroom-csrf-token"
ADMIN_PANEL_DIR_NAME = "ADMIN_PANEL"
SERVER_ADMINISTRATOR_IDS_SETTING = "server_administrator_player_ids"
FILTERS = True
FILTER_REPLACEMENT = "#@(!@#"
FILTER_ALLOWED_WORDS: set[str] = set()
FILTER_SNAPSHOT_DIR_NAME = "FILTERS"
DEFAULT_MAX_MAINTENANCE_MINUTES = 10_080
MAINTENANCE_SUPPORTED_API_VERSIONS = ("25april2019",)
ERROR_WEBHOOK_ENV_NAMES = ("RECROOM_ERROR_WEBHOOK_URL", "RECROOM_API_ERROR_WEBHOOK_URL", "DISCORD_ERROR_WEBHOOK_URL")
DISCORD_RED_COLOR = 0xFF0000
WEBHOOK_ALERT_TITLE = "⋆｡°✩ Endpoint ghost detected ✩°｡⋆"
WEBHOOK_ALERT_MESSAGE = "Someone was waiting for a ghost. ."
MAX_WEBHOOK_FIELD_VALUE_LENGTH = 900
SENSITIVE_PAYLOAD_KEY_RE = re.compile(r"(?i)(token|secret|password|authorization|webhook|cookie|session|email|admin)")


class ConfigurationError(RuntimeError):
    pass


class ModerationReasonRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=2000)
    idempotency_key: str | None = Field(default=None, max_length=200)


class ModerationAssignmentRequest(ModerationReasonRequest):
    assigned_to: str | None = Field(default=None, max_length=200)


class ModerationTimeoutRequest(ModerationReasonRequest):
    duration_seconds: int = Field(
        ge=moderation_service.MIN_TIMEOUT_SECONDS,
        le=moderation_service.MAX_TIMEOUT_SECONDS,
    )


class ModerationReversalRequest(ModerationReasonRequest):
    action_id: str | None = Field(default=None, max_length=100)


class ModerationActionRequest(BaseModel):
    case_id: str | None = Field(default=None, max_length=100)
    target_type: str = Field(min_length=1, max_length=50)
    target_id: str = Field(min_length=1, max_length=200)
    action: str = Field(min_length=1, max_length=100)
    duration_seconds: int | None = Field(
        default=None,
        ge=moderation_service.MIN_TIMEOUT_SECONDS,
        le=moderation_service.MAX_TIMEOUT_SECONDS,
    )
    reason: str = Field(min_length=1, max_length=2000)
    idempotency_key: str = Field(min_length=8, max_length=200)
    confirmation: str | None = Field(default=None, max_length=500)


class BugReportDismissRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=2000)
    idempotency_key: str = Field(min_length=8, max_length=200)


class BugReportGroupRequest(BaseModel):
    report_ids: list[str] = Field(min_length=1, max_length=100)
    group_id: str | None = Field(default=None, max_length=100)
    title: str | None = Field(default=None, max_length=500)
    reason: str = Field(min_length=1, max_length=2000)
    idempotency_key: str = Field(min_length=8, max_length=200)


class BugReportUngroupRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=2000)
    idempotency_key: str = Field(min_length=8, max_length=200)


@dataclass(frozen=True)
class Settings:
    root_dir: Path
    api_dir: Path
    data_dir: Path
    db_path: Path
    is_railway: bool
    port: int
    host: str
    ban_hash_pepper: str
    max_request_body_bytes: int
    error_webhook_url: str | None


def _is_railway_environment() -> bool:
    railway_markers = (
        "RAILWAY_ENVIRONMENT",
        "RAILWAY_ENVIRONMENT_NAME",
        "RAILWAY_PROJECT_ID",
        "RAILWAY_SERVICE_ID",
        "RAILWAY_VOLUME_MOUNT_PATH",
        "RAILWAY_VOLUME_PATH",
    )
    return any(os.getenv(name) for name in railway_markers)


def _resolve_data_dir(root_dir: Path, is_railway: bool) -> Path:
    data_dir = os.getenv("DATA_DIR")
    if data_dir:
        return Path(data_dir).expanduser().resolve()

    railway_volume = os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or os.getenv("RAILWAY_VOLUME_PATH")
    if railway_volume:
        return (Path(railway_volume).expanduser().resolve() / "DATA")

    if is_railway:
        raise ConfigurationError(
            "Railway/container mode requires persistent storage. Set DATA_DIR, "
            "RAILWAY_VOLUME_MOUNT_PATH, or RAILWAY_VOLUME_PATH."
        )

    return root_dir / "DATA"


def _read_port(default: int = DEFAULT_LOCAL_PORT) -> int:
    raw_port = os.getenv("PORT")
    if not raw_port:
        return default
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise ConfigurationError("PORT must be an integer.") from exc
    if not 1 <= port <= 65535:
        raise ConfigurationError("PORT must be between 1 and 65535.")
    return port


def _read_max_request_body_bytes(default: int = DEFAULT_MAX_REQUEST_BODY_BYTES) -> int:
    raw_value = os.getenv("RECROOM_MAX_REQUEST_BODY_BYTES") or os.getenv("MAX_REQUEST_BODY_BYTES")
    if not raw_value:
        return default
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ConfigurationError("MAX_REQUEST_BODY_BYTES must be an integer.") from exc
    if value < 0:
        raise ConfigurationError("MAX_REQUEST_BODY_BYTES must be zero or greater.")
    return value


def _read_error_webhook_url() -> str | None:
    for name in ERROR_WEBHOOK_ENV_NAMES:
        value = os.getenv(name)
        if value:
            value = value.strip()
            if value:
                return value
    return None


def _first_header_value(value: str | None) -> str:
    return str(value or "").split(",", 1)[0].strip()


def _normalize_origin(value: str) -> str:
    value = value.strip().rstrip("/")
    if not value:
        return ""
    if "://" not in value:
        value = f"https://{value}"
    parsed = urllib.parse.urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _configured_public_base_url() -> str | None:
    for name in PUBLIC_BASE_URL_ENV_NAMES:
        value = os.getenv(name)
        if value:
            normalized = _normalize_origin(value)
            if normalized:
                return normalized

    railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN")
    if railway_domain:
        normalized = _normalize_origin(railway_domain)
        if normalized:
            return normalized
    return None


def _normalize_websocket_origin(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if "://" not in value:
        value = f"ws://{value}"
    parsed = urllib.parse.urlsplit(value)
    scheme = parsed.scheme.casefold()
    if scheme == "http":
        scheme = "ws"
    elif scheme == "https":
        scheme = "wss"
    if scheme not in {"ws", "wss"} or not parsed.netloc:
        return ""
    return urllib.parse.urlunsplit(
        (scheme, parsed.netloc, parsed.path.rstrip("/"), "", "")
    )


def _configured_public_websocket_url() -> str | None:
    for name in PUBLIC_WEBSOCKET_URL_ENV_NAMES:
        value = os.getenv(name)
        if value:
            normalized = _normalize_websocket_origin(value)
            if normalized:
                return normalized

    railway_domain = os.getenv("RAILWAY_TCP_PROXY_DOMAIN")
    railway_port = os.getenv("RAILWAY_TCP_PROXY_PORT")
    if railway_domain and railway_port:
        normalized = _normalize_websocket_origin(
            f"ws://{railway_domain}:{railway_port}"
        )
        if normalized:
            return normalized
    return None


def _request_origin(request: Request, settings: Any | None = None) -> str:
    proto = _first_header_value(request.headers.get("x-forwarded-proto")) or request.url.scheme or "http"
    host = (
        _first_header_value(request.headers.get("x-forwarded-host"))
        or _first_header_value(request.headers.get("host"))
        or request.url.netloc
    )
    host_lower = host.casefold()
    if (
        not host
        or host_lower.startswith("testserver")
        or host_lower.startswith("0.0.0.0")
        or host_lower in {"::", "[::]"}
    ):
        port = getattr(settings, "port", DEFAULT_LOCAL_PORT)
        host = f"localhost:{port}"
        proto = "http"
    return f"{proto}://{host}".rstrip("/")


def _api_version_date(api_version: str) -> datetime | None:
    match = re.fullmatch(
        r"(\d{1,2})"
        r"(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)"
        r"(\d{4})",
        api_version.strip().casefold(),
    )
    if match is None:
        return None
    month_number = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }[match.group(2)]
    try:
        return datetime(int(match.group(3)), month_number, int(match.group(1)))
    except ValueError:
        return None


def _local_transport_policy(settings: Any) -> dict[str, Any]:
    policy_path = (
        Path(settings.root_dir) / "TLS" / LOCAL_TRANSPORT_POLICY_FILE
    )
    try:
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        policy = {}
    return policy if isinstance(policy, dict) else {}


def _local_api_version_scheme(settings: Any, api_version: str) -> str:
    policy = _local_transport_policy(settings)
    normalized_version = api_version.strip().casefold()
    http_versions = {
        str(value).strip().casefold()
        for value in policy.get("http_api_versions", [])
        if isinstance(value, str) and value.strip()
    } if isinstance(policy.get("http_api_versions", []), list) else set()
    https_versions = {
        str(value).strip().casefold()
        for value in policy.get("https_api_versions", [])
        if isinstance(value, str) and value.strip()
    } if isinstance(policy.get("https_api_versions", []), list) else set()

    if normalized_version in http_versions:
        return "http"
    if normalized_version in https_versions:
        return "https"

    threshold_value = policy.get(
        "https_from_date",
        DEFAULT_LOCAL_HTTPS_FROM_DATE,
    )
    if threshold_value is not None:
        try:
            threshold = datetime.strptime(
                str(threshold_value),
                "%Y-%m-%d",
            )
        except ValueError:
            threshold = datetime.strptime(DEFAULT_LOCAL_HTTPS_FROM_DATE, "%Y-%m-%d")
        version_date = _api_version_date(normalized_version)
        if version_date is not None and version_date >= threshold:
            return "https"

    default_scheme = str(policy.get("default_scheme") or "http").casefold()
    return default_scheme if default_scheme in {"http", "https"} else "http"


def public_api_origin(request: Request, context: Any, api_version: str) -> str:
    settings = getattr(context, "settings", None)
    if getattr(settings, "is_railway", False):
        return _configured_public_base_url() or DEFAULT_RAILWAY_PUBLIC_BASE_URL

    origin = _request_origin(request, settings)
    if settings is None:
        return origin
    parsed = urllib.parse.urlsplit(origin)
    scheme = _local_api_version_scheme(settings, api_version)
    return urllib.parse.urlunsplit(
        (scheme, parsed.netloc, parsed.path, "", "")
    ).rstrip("/")


def public_api_base_url(request: Request, context: Any, api_version: str) -> str:
    origin = public_api_origin(request, context, api_version)
    return f"{origin.rstrip('/')}/{api_version.strip('/')}/"


def public_websocket_origin(request: Request, context: Any, api_version: str) -> str:
    settings = getattr(context, "settings", None)
    if getattr(settings, "is_railway", False):
        return (
            _configured_public_websocket_url()
            or DEFAULT_RAILWAY_PUBLIC_WEBSOCKET_URL
        )

    api_origin = public_api_origin(request, context, api_version)
    parsed = urllib.parse.urlsplit(api_origin)
    scheme = "wss" if parsed.scheme.casefold() == "https" else "ws"
    return urllib.parse.urlunsplit(
        (scheme, parsed.netloc, parsed.path.rstrip("/"), "", "")
    )


def public_websocket_base_url(request: Request, context: Any, api_version: str) -> str:
    origin = public_websocket_origin(request, context, api_version)
    return f"{origin.rstrip('/')}/{api_version.strip('/')}/"


def load_settings() -> Settings:
    root_dir = Path(__file__).resolve().parent
    is_railway = _is_railway_environment()
    default_host = "0.0.0.0" if is_railway else "127.0.0.1"
    ban_hash_pepper = os.getenv("RECROOM_BAN_HASH_PEPPER") or os.getenv("BAN_HASH_PEPPER")
    if is_railway and not ban_hash_pepper:
        raise ConfigurationError(
            "Railway/container mode requires RECROOM_BAN_HASH_PEPPER or BAN_HASH_PEPPER for ban identity hashing."
        )
    if not ban_hash_pepper:
        ban_hash_pepper = "local-development-ban-pepper"
    api_dir = root_dir / "APIs"
    data_dir = _resolve_data_dir(root_dir, is_railway)
    return Settings(
        root_dir=root_dir,
        api_dir=api_dir,
        data_dir=data_dir,
        db_path=data_dir / "database.sqlite3",
        is_railway=is_railway,
        port=_read_port(),
        host=os.getenv("HOST", default_host),
        ban_hash_pepper=ban_hash_pepper,
        max_request_body_bytes=_read_max_request_body_bytes(),
        error_webhook_url=_read_error_webhook_url(),
    )


def refresh_railway_content_filter_snapshot(settings: Settings) -> None:
    if not settings.is_railway:
        return
    try:
        from tools.update_bad_word_lists import (
            DEFAULT_COMMIT,
            DEFAULT_LANGUAGES,
            update,
        )

        update(
            settings.root_dir / FILTER_SNAPSHOT_DIR_NAME,
            DEFAULT_COMMIT,
            DEFAULT_LANGUAGES,
        )
    except Exception as exc:
        # ContentFilter validates the last complete snapshot and fails closed
        # when none is available. A transient upstream outage must never make
        # the server silently accept unfiltered writes.
        print(
            "Railway content-filter refresh failed; filtered writes will use "
            f"the last valid snapshot or fail closed ({type(exc).__name__}).",
            file=sys.stderr,
        )


def ensure_runtime_directories(settings: Settings) -> dict[str, str]:
    settings.api_dir.mkdir(parents=True, exist_ok=True)
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    image_dir = settings.data_dir / IMAGE_DATA_DIR_NAME
    image_dir.mkdir(parents=True, exist_ok=True)
    (image_dir / BACKEND_IMAGE_DIR_NAME).mkdir(parents=True, exist_ok=True)
    (image_dir / PLAYER_IMAGE_DIR_NAME).mkdir(parents=True, exist_ok=True)
    (settings.data_dir / VIDEO_DATA_DIR_NAME).mkdir(parents=True, exist_ok=True)
    legacy_image_moves = migrate_legacy_root_images(settings.data_dir)
    enforce_data_directory_policy(settings.data_dir)
    return legacy_image_moves


def is_allowed_data_file(path: Path, data_dir: Path) -> bool:
    resolved = path.resolve()
    data_root = data_dir.resolve()
    try:
        relative = resolved.relative_to(data_root)
    except ValueError:
        return False
    if not resolved.is_file():
        return False
    name = resolved.name
    if len(relative.parts) == 1 and SQLITE_SIDECAR_RE.match(name):
        return True
    if len(relative.parts) == 1:
        return resolved.suffix.lower() in ALLOWED_DATA_ROOT_EXTENSIONS
    if len(relative.parts) >= 2:
        if (
            relative.parts[0].casefold() == VIDEO_DATA_DIR_NAME.casefold()
            and len(relative.parts) == 2
        ):
            return resolved.suffix.lower() in ALLOWED_VIDEO_EXTENSIONS
        return resolved.suffix.lower() in (ALLOWED_IMAGE_EXTENSIONS | ALLOWED_DATA_ROOT_EXTENSIONS)
    return False


def enforce_data_directory_policy(data_dir: Path) -> None:
    for child in data_dir.iterdir():
        if child.is_dir():
            for nested in child.rglob("*"):
                if nested.is_file() and not is_allowed_data_file(nested, data_dir):
                    raise ConfigurationError(f"Unsupported file in DATA/{child.name}: {nested.name}")
            continue
        if child.is_file() and not is_allowed_data_file(child, data_dir):
            raise ConfigurationError(f"Unsupported file in DATA: {child.name}")


def validate_data_write_path(data_dir: Path, filename: str) -> Path:
    if Path(filename).name != filename:
        raise ValueError("DATA filename must not include path separators.")
    path = (data_dir / filename).resolve()
    if not is_allowed_data_filename(path.name):
        raise ValueError("DATA root only accepts .json and database.sqlite3 files.")
    if data_dir.resolve() not in path.parents:
        raise ValueError("DATA write path escaped the DATA directory.")
    return path


def is_allowed_data_filename(filename: str) -> bool:
    if SQLITE_SIDECAR_RE.match(filename):
        return True
    return Path(filename).suffix.lower() in ALLOWED_DATA_ROOT_EXTENSIONS


def validate_image_write_path(data_dir: Path, filename: str, bucket_name: str) -> Path:
    if Path(filename).name != filename:
        raise ValueError("Image filename must not include path separators.")
    if Path(filename).suffix.lower() not in ALLOWED_IMAGE_EXTENSIONS:
        raise ValueError("DATA/IMAGES only accepts .png, .jpg, and .jpeg files.")
    if bucket_name not in IMAGE_BUCKET_NAMES:
        raise ValueError("Unknown image storage bucket.")
    image_dir = (data_dir / IMAGE_DATA_DIR_NAME).resolve()
    image_dir.mkdir(parents=True, exist_ok=True)
    bucket_dir = (image_dir / bucket_name).resolve()
    bucket_dir.mkdir(parents=True, exist_ok=True)
    path = (bucket_dir / filename).resolve()
    if image_dir not in path.parents:
        raise ValueError("Image write path escaped DATA/IMAGES.")
    return path


def migrate_legacy_root_images(data_dir: Path) -> dict[str, str]:
    image_dir = data_dir / IMAGE_DATA_DIR_NAME
    image_dir.mkdir(parents=True, exist_ok=True)
    backend_image_dir = image_dir / BACKEND_IMAGE_DIR_NAME
    backend_image_dir.mkdir(parents=True, exist_ok=True)
    moved: dict[str, str] = {}
    for child in list(data_dir.iterdir()):
        if child.is_file() and child.suffix.lower() in ALLOWED_IMAGE_EXTENSIONS:
            target = backend_image_dir / child.name
            if target.exists() and target.resolve() != child.resolve():
                target = backend_image_dir / f"{child.stem}-{uuid.uuid4().hex}{child.suffix.lower()}"
            if target.resolve() != child.resolve():
                child.replace(target)
                moved[child.name] = f"{IMAGE_DATA_DIR_NAME}/{BACKEND_IMAGE_DIR_NAME}/{target.name}"
        elif child.is_dir() and child.name != IMAGE_DATA_DIR_NAME:
            for nested in list(child.iterdir()):
                if nested.is_file() and nested.suffix.lower() in ALLOWED_IMAGE_EXTENSIONS:
                    target = backend_image_dir / nested.name
                    if target.exists() and target.resolve() != nested.resolve():
                        target = backend_image_dir / f"{nested.stem}-{uuid.uuid4().hex}{nested.suffix.lower()}"
                    if target.resolve() != nested.resolve():
                        nested.replace(target)
                        relative_path = f"{IMAGE_DATA_DIR_NAME}/{BACKEND_IMAGE_DIR_NAME}/{target.name}"
                        moved[f"{child.name}/{nested.name}"] = relative_path
                        moved[nested.name] = relative_path
            if not any(child.iterdir()):
                try:
                    child.rmdir()
                except OSError:
                    pass
    return moved


def migrate_legacy_data_asset_records(db: Database, legacy_image_moves: dict[str, str]) -> None:
    updates = dict(legacy_image_moves)
    with db.connection() as conn:
        rows = conn.execute("SELECT relative_path FROM data_assets").fetchall()
    for row in rows:
        relative_path = row["relative_path"]
        if "/" not in relative_path and "\\" not in relative_path and Path(relative_path).suffix.lower() in ALLOWED_IMAGE_EXTENSIONS:
            updates.setdefault(
                relative_path,
                f"{IMAGE_DATA_DIR_NAME}/{BACKEND_IMAGE_DIR_NAME}/{relative_path}",
            )
    if not updates:
        return
    with db.transaction() as conn:
        for old_path, new_path in updates.items():
            conn.execute(
                """
                UPDATE data_assets
                SET relative_path = ?
                WHERE relative_path = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM data_assets AS existing
                      WHERE existing.relative_path = ?
                  )
                """,
                (new_path, old_path, new_path),
            )


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def utc_datetime_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_datetime_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a UTC offset.")
    return parsed.astimezone(timezone.utc)


def maintenance_remaining_minutes(starts_at_utc: str | None, *, now: datetime | None = None) -> int:
    if not starts_at_utc:
        return 0
    now = (now or utc_datetime_now()).astimezone(timezone.utc)
    remaining_seconds = (parse_utc_datetime(starts_at_utc) - now).total_seconds()
    return max(0, int(math.ceil(remaining_seconds / 60.0)))


def _truncate_webhook_value(value: str, limit: int = MAX_WEBHOOK_FIELD_VALUE_LENGTH) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)] + "..."


def _normalize_alert_route_path(route_path: str) -> str:
    clean_path = route_path.split("?", 1)[0].strip("/")
    return "/" + clean_path if clean_path else "/"


def endpoint_alert_key(method: str, route_path: str) -> str:
    normalized = f"{method.upper()} {_normalize_alert_route_path(route_path).casefold()}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _summarize_payload_shape(value: Any, depth: int = 0) -> Any:
    if depth >= 2:
        return type(value).__name__
    if isinstance(value, dict):
        result = {}
        for key, item in list(value.items())[:20]:
            key_text = str(key)
            if SENSITIVE_PAYLOAD_KEY_RE.search(key_text):
                result[key_text] = "[redacted]"
            else:
                result[key_text] = _summarize_payload_shape(item, depth + 1)
        return result
    if isinstance(value, list):
        if not value:
            return []
        return [f"{len(value)} item(s)", _summarize_payload_shape(value[0], depth + 1)]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return f"string({len(value)} chars)"
    return type(value).__name__


async def summarize_request_data(request: Request, route_path: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "method": request.method.upper(),
        "endpoint": _normalize_alert_route_path(route_path),
    }
    query_keys = sorted({str(key) for key in request.query_params.keys()})
    if query_keys:
        summary["query_keys"] = query_keys[:30]
    content_type = str(request.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
    if content_type:
        summary["content_type"] = content_type
    try:
        body = await request.body()
    except Exception:
        body = b""
        summary["body"] = "unavailable"
    if not body:
        summary.setdefault("body", "empty")
        return summary
    summary["body_bytes"] = len(body)
    if content_type == "application/json":
        try:
            summary["json_shape"] = _summarize_payload_shape(json.loads(body.decode("utf-8")))
        except Exception:
            summary["body"] = "invalid json"
    elif content_type in {"application/x-www-form-urlencoded", "multipart/form-data"}:
        summary["body"] = "form data present; values redacted"
    elif content_type.startswith("text/") and len(body) <= 256:
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            summary["body"] = "binary data present"
        else:
            summary["text_preview"] = _truncate_webhook_value(text.replace("\r", "\\r").replace("\n", "\\n"), 256)
    else:
        summary["body"] = "binary or large data present"
    return summary


class Database:
    def __init__(self, path: Path):
        self.path = path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


MIGRATIONS: tuple[tuple[int, str], ...] = (
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS players (
            player_id TEXT PRIMARY KEY,
            username TEXT UNIQUE,
            display_name TEXT,
            email TEXT NOT NULL,
            verified INTEGER NOT NULL,
            permissions_json TEXT NOT NULL,
            canonical_level INTEGER NOT NULL,
            canonical_xp INTEGER NOT NULL,
            profile_picture_asset_id TEXT,
            is_coach INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS player_version_state (
            player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
            api_version TEXT NOT NULL,
            state_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (player_id, api_version)
        );

        CREATE TABLE IF NOT EXISTS rooms (
            room_id TEXT PRIMARY KEY,
            owner_player_id TEXT REFERENCES players(player_id) ON DELETE SET NULL,
            name TEXT NOT NULL,
            is_official INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS inventory_items (
            player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
            item_key TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            state_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (player_id, item_key)
        );

        CREATE TABLE IF NOT EXISTS gift_boxes (
            gift_box_id TEXT PRIMARY KEY,
            player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
            state_json TEXT NOT NULL,
            opened INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS data_assets (
            asset_id TEXT PRIMARY KEY,
            owner_player_id TEXT REFERENCES players(player_id) ON DELETE SET NULL,
            relative_path TEXT NOT NULL UNIQUE,
            mime_type TEXT NOT NULL,
            file_ext TEXT NOT NULL,
            purpose TEXT,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS server_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
    ),
    (
        2,
        """
        INSERT INTO server_settings(key, value_json, created_at, updated_at)
        SELECT
            'motd',
            value_json,
            strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
            strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        FROM server_settings
        WHERE key LIKE '%.motd'
          AND NOT EXISTS (SELECT 1 FROM server_settings WHERE key = 'motd')
        ORDER BY created_at ASC
        LIMIT 1;
        """,
    ),
    (
        3,
        """
        ALTER TABLE players ADD COLUMN is_banned INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE players ADD COLUMN banned_at TEXT NULL;
        ALTER TABLE players ADD COLUMN ban_reason TEXT NULL;

        CREATE TABLE IF NOT EXISTS player_identities (
            player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
            identity_type TEXT NOT NULL,
            identity_hash TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            PRIMARY KEY (identity_type, identity_hash, player_id)
        );

        CREATE INDEX IF NOT EXISTS idx_player_identities_lookup
            ON player_identities(identity_type, identity_hash);

        CREATE TABLE IF NOT EXISTS bans (
            id TEXT PRIMARY KEY,
            player_id TEXT NULL REFERENCES players(player_id) ON DELETE SET NULL,
            identity_type TEXT NOT NULL,
            identity_hash TEXT NOT NULL,
            reason TEXT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_bans_lookup
            ON bans(identity_type, identity_hash, active);

        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
            state_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
    ),
    (
        4,
        """
        ALTER TABLE rooms ADD COLUMN creator_player_id TEXT REFERENCES players(player_id) ON DELETE SET NULL;
        ALTER TABLE rooms ADD COLUMN is_coach_only_edit INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE rooms ADD COLUMN created_by_system INTEGER NOT NULL DEFAULT 0;
        """,
    ),
    (
        5,
        """
        CREATE TABLE IF NOT EXISTS room_data_blobs (
            blob_name TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            owner_player_id TEXT REFERENCES players(player_id) ON DELETE SET NULL,
            data BLOB NOT NULL,
            image_list_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_room_data_blobs_room
            ON room_data_blobs(room_id, updated_at);
        """,
    ),
    (
        6,
        """
        CREATE TABLE IF NOT EXISTS endpoint_error_alerts (
            endpoint_key TEXT PRIMARY KEY,
            method TEXT NOT NULL,
            route_path TEXT NOT NULL,
            api_versions_json TEXT NOT NULL,
            latest_api_version TEXT NOT NULL,
            latest_adapter_file TEXT NOT NULL,
            latest_status_code INTEGER NOT NULL,
            request_count INTEGER NOT NULL,
            webhook_message_id TEXT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_request_summary_json TEXT NOT NULL,
            last_error_detail TEXT NOT NULL
        );
        """,
    ),
    (
        7,
        """
        CREATE TABLE IF NOT EXISTS moderation_cases (
            case_id TEXT PRIMARY KEY,
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            canonical_category TEXT NOT NULL,
            state TEXT NOT NULL,
            report_count INTEGER NOT NULL DEFAULT 0,
            counting_report_count INTEGER NOT NULL DEFAULT 0,
            assigned_to TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            closed_at TEXT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_moderation_cases_queue
            ON moderation_cases(state, updated_at);

        CREATE INDEX IF NOT EXISTS idx_moderation_cases_target
            ON moderation_cases(target_type, target_id, canonical_category, updated_at);

        CREATE TABLE IF NOT EXISTS moderation_reports (
            report_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL REFERENCES moderation_cases(case_id),
            reporter_player_id TEXT NOT NULL REFERENCES players(player_id),
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            canonical_category TEXT NOT NULL,
            raw_category_json TEXT NOT NULL,
            category_schema TEXT NOT NULL,
            public_details TEXT NOT NULL,
            protected_evidence_id TEXT NULL,
            room_id TEXT NULL,
            game_session_id TEXT NULL,
            source_version TEXT NOT NULL,
            source_endpoint TEXT NOT NULL,
            source_schema TEXT NOT NULL,
            source_payload_json TEXT NOT NULL,
            client_request_id TEXT NULL,
            duplicate_of TEXT NULL REFERENCES moderation_reports(report_id),
            reporter_cluster_id TEXT NULL,
            counts_toward_case_score INTEGER NOT NULL DEFAULT 1,
            evidence_status TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_moderation_reports_case
            ON moderation_reports(case_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_moderation_reports_duplicate
            ON moderation_reports(
                reporter_player_id, target_type, target_id,
                canonical_category, room_id, game_session_id, created_at
            );

        CREATE TABLE IF NOT EXISTS moderation_evidence (
            evidence_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL REFERENCES moderation_cases(case_id),
            report_id TEXT NOT NULL,
            evidence_type TEXT NOT NULL,
            restricted INTEGER NOT NULL DEFAULT 1,
            public_text TEXT NULL,
            raw_text TEXT NULL,
            sha256 TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            retention_until TEXT NULL,
            deleted_at TEXT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_moderation_evidence_case
            ON moderation_evidence(case_id, created_at);

        CREATE TABLE IF NOT EXISTS moderation_sanctions (
            sanction_id TEXT PRIMARY KEY,
            case_id TEXT NULL REFERENCES moderation_cases(case_id),
            target_player_id TEXT NOT NULL REFERENCES players(player_id),
            sanction_type TEXT NOT NULL,
            scope TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            starts_at TEXT NOT NULL,
            expires_at TEXT NULL,
            reason TEXT NOT NULL,
            created_by TEXT NOT NULL,
            reversed_by_action_id TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_moderation_sanctions_active
            ON moderation_sanctions(target_player_id, active, expires_at);

        CREATE TABLE IF NOT EXISTS moderation_actions (
            action_id TEXT PRIMARY KEY,
            case_id TEXT NULL REFERENCES moderation_cases(case_id),
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            actor_type TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            action TEXT NOT NULL,
            previous_state TEXT NULL,
            new_state TEXT NULL,
            reason TEXT NULL,
            duration_seconds INTEGER NULL,
            idempotency_key TEXT NULL UNIQUE,
            reverses_action_id TEXT NULL REFERENCES moderation_actions(action_id),
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_moderation_actions_case
            ON moderation_actions(case_id, created_at);

        CREATE TABLE IF NOT EXISTS maintenance_state (
            singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
            active INTEGER NOT NULL,
            starts_at_utc TEXT NULL,
            revision INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            updated_by TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS maintenance_audit (
            audit_id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            revision INTEGER NOT NULL,
            actor_id TEXT NOT NULL,
            reason TEXT NULL,
            idempotency_key TEXT NULL UNIQUE,
            previous_state_json TEXT NOT NULL,
            new_state_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS timed_content_schedules (
            schedule_key TEXT PRIMARY KEY,
            model TEXT NOT NULL,
            revision INTEGER NOT NULL,
            catalog_revision TEXT NOT NULL,
            config_json TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS timed_content_periods (
            schedule_key TEXT NOT NULL REFERENCES timed_content_schedules(schedule_key),
            period_id TEXT NOT NULL,
            schedule_revision INTEGER NOT NULL,
            catalog_revision TEXT NOT NULL,
            period_index INTEGER NOT NULL,
            starts_at_utc TEXT NOT NULL,
            ends_at_utc TEXT NOT NULL,
            content_json TEXT NOT NULL,
            materialized_at TEXT NOT NULL,
            PRIMARY KEY(schedule_key, period_id)
        );

        CREATE TABLE IF NOT EXISTS timed_content_player_progress (
            player_id TEXT NOT NULL REFERENCES players(player_id),
            schedule_key TEXT NOT NULL,
            period_id TEXT NOT NULL,
            state_json TEXT NOT NULL,
            reward_claimed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(player_id, schedule_key, period_id)
        );

        CREATE TABLE IF NOT EXISTS content_filter_events (
            event_id TEXT PRIMARY KEY,
            player_id TEXT NULL REFERENCES players(player_id),
            source_version TEXT NULL,
            field_context TEXT NOT NULL,
            policy TEXT NOT NULL,
            changed INTEGER NOT NULL,
            blocked INTEGER NOT NULL,
            match_count INTEGER NOT NULL,
            list_version TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """,
    ),
    (
        8,
        """
        CREATE TABLE IF NOT EXISTS moderation_content_controls (
            control_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL REFERENCES moderation_cases(case_id),
            originating_action_id TEXT NOT NULL REFERENCES moderation_actions(action_id),
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            control_type TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            reason TEXT NOT NULL,
            created_by TEXT NOT NULL,
            reversed_by_action_id TEXT NULL REFERENCES moderation_actions(action_id),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_moderation_content_controls_target
            ON moderation_content_controls(
                target_type, target_id, control_type, active
            );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_moderation_content_controls_case_active
            ON moderation_content_controls(case_id, control_type)
            WHERE active = 1;
        """,
    ),
    (
        9,
        """
        CREATE TABLE IF NOT EXISTS admin_sessions (
            session_id TEXT PRIMARY KEY,
            token_hash TEXT NOT NULL UNIQUE,
            csrf_token TEXT NOT NULL,
            secret_fingerprint TEXT NOT NULL,
            operator_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL,
            revoked_at TEXT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_admin_sessions_token
            ON admin_sessions(token_hash, revoked_at, expires_at);
        """,
    ),
    (
        10,
        """
        CREATE TABLE IF NOT EXISTS bug_report_groups (
            group_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'open',
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bug_reports (
            report_id TEXT PRIMARY KEY,
            reporter_player_id TEXT NULL REFERENCES players(player_id),
            reporter_legacy_id INTEGER NOT NULL,
            summary TEXT NOT NULL,
            description TEXT NOT NULL,
            test_case_key TEXT NOT NULL,
            build_version TEXT NOT NULL,
            build_timestamp INTEGER NOT NULL,
            bundle_version_code INTEGER NULL,
            screenshot_blob_name TEXT NULL,
            output_log_blob_name TEXT NULL,
            group_id TEXT NULL REFERENCES bug_report_groups(group_id),
            status TEXT NOT NULL DEFAULT 'open',
            dismissed_by TEXT NULL,
            dismissed_at TEXT NULL,
            dismiss_reason TEXT NULL,
            source_version TEXT NOT NULL,
            source_endpoint TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_bug_reports_queue
            ON bug_reports(status, created_at);

        CREATE INDEX IF NOT EXISTS idx_bug_reports_group
            ON bug_reports(group_id, created_at);

        CREATE TABLE IF NOT EXISTS bug_report_actions (
            action_id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            report_id TEXT NULL REFERENCES bug_reports(report_id),
            group_id TEXT NULL REFERENCES bug_report_groups(group_id),
            reason TEXT NULL,
            previous_state TEXT NULL,
            new_state TEXT NULL,
            idempotency_key TEXT NULL UNIQUE,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_bug_report_actions_report
            ON bug_report_actions(report_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_bug_report_actions_group
            ON bug_report_actions(group_id, created_at);
        """,
    ),
)


def initialize_database(db: Database) -> None:
    with db.transaction() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        applied = {row["version"] for row in conn.execute("SELECT version FROM schema_migrations")}
        for version, sql in MIGRATIONS:
            if version not in applied:
                for statement in sql.split(";"):
                    statement = statement.strip()
                    if statement:
                        conn.execute(statement)
                conn.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (version, utc_now()),
                )
    ensure_default_server_settings(db)
    normalize_shared_server_settings(db)
    ensure_coach_profile(db)
    cleanup_existing_banned_players(db)


def ensure_default_server_settings(db: Database) -> None:
    """Seed database-backed settings that should exist even without Railway variables."""
    now = utc_now()
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO server_settings(key, value_json, created_at, updated_at)
            VALUES ('motd', ?, ?, ?)
            ON CONFLICT(key) DO NOTHING
            """,
            (json.dumps(""), now, now),
        )


def normalize_shared_server_settings(db: Database) -> None:
    """Normalize old build-local global settings into shared canonical server settings.

    Live MOTD storage is intentionally build-neutral. Older deployments may
    contain keys such as MOTD2016.motd or 11august2016v1.motd; those are
    migration leftovers, not separate live data spaces. If the shared MOTD is
    empty, preserve the first non-empty legacy value, then remove the legacy
    MOTD keys so all builds read the same canonical value.
    """
    with db.transaction() as conn:
        shared_row = conn.execute("SELECT value_json FROM server_settings WHERE key = 'motd'").fetchone()
        shared_value = None
        if shared_row is not None:
            try:
                shared_value = json.loads(shared_row["value_json"])
            except Exception:
                shared_value = shared_row["value_json"]
        if not isinstance(shared_value, str) or shared_value == "":
            legacy_rows = conn.execute(
                """
                SELECT key, value_json
                FROM server_settings
                WHERE key LIKE '%.motd'
                ORDER BY updated_at DESC, created_at DESC
                """
            ).fetchall()
            for row in legacy_rows:
                try:
                    value = json.loads(row["value_json"])
                except Exception:
                    value = row["value_json"]
                if isinstance(value, str) and value != "":
                    conn.execute(
                        """
                        INSERT INTO server_settings(key, value_json, created_at, updated_at)
                        VALUES ('motd', ?, ?, ?)
                        ON CONFLICT(key) DO UPDATE
                        SET value_json = excluded.value_json, updated_at = excluded.updated_at
                        """,
                        (json.dumps(value), utc_now(), utc_now()),
                    )
                    break
        conn.execute("DELETE FROM server_settings WHERE key LIKE '%.motd'")


def cleanup_existing_banned_players(db: Database) -> None:
    context = ServerContext(load_settings(), db)
    with db.connection() as conn:
        rows = conn.execute("SELECT player_id FROM players WHERE is_banned = 1").fetchall()
    for row in rows:
        context.enforce_ban_cleanup(row["player_id"])


def row_to_canonical_player(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "player_id": row["player_id"],
        "username": row["username"],
        "display_name": row["display_name"],
        "email": row["email"],
        "verified": bool(row["verified"]),
        "permissions": json.loads(row["permissions_json"]),
        "canonical_level": int(row["canonical_level"]),
        "canonical_xp": int(row["canonical_xp"]),
        "profile_picture_asset_id": row["profile_picture_asset_id"],
        "is_coach": bool(row["is_coach"]),
        "is_banned": bool(row["is_banned"]),
        "banned_at": row["banned_at"],
        "ban_reason": row["ban_reason"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def ensure_coach_profile(db: Database) -> dict[str, Any]:
    now = utc_now()
    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM players WHERE player_id = ?", (COACH_PLAYER_ID,)).fetchone()
        if row is None:
            conn.execute(
                """
                INSERT INTO players (
                    player_id, username, display_name, email, verified, permissions_json,
                    canonical_level, canonical_xp, profile_picture_asset_id, is_coach,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    COACH_PLAYER_ID,
                    "Coach",
                    "Coach",
                    DEFAULT_CREATED_PLAYER_EMAIL,
                    1,
                    json.dumps(DEV_PERMISSIONS),
                    99,
                    0,
                    None,
                    1,
                    now,
                    now,
                ),
            )
            row = conn.execute("SELECT * FROM players WHERE player_id = ?", (COACH_PLAYER_ID,)).fetchone()
    return row_to_canonical_player(row)


def normalize_identity_value(value: Any) -> str:
    return str(value or "").strip().casefold()


def hash_ban_identity(pepper: str, identity_type: str, value: Any) -> str:
    normalized = normalize_identity_value(value)
    if not normalized:
        return ""
    payload = f"{identity_type}:{normalized}".encode("utf-8")
    return hashlib.sha256(pepper.encode("utf-8") + b":" + payload).hexdigest()


def configured_admin_key() -> str | None:
    for name in ADMIN_KEY_ENV_NAMES:
        value = os.getenv(name)
        if value:
            return value
    return None


def admin_session_ttl_seconds() -> int:
    raw_value = os.getenv("RECROOM_ADMIN_SESSION_TTL_SECONDS")
    if raw_value is None:
        return DEFAULT_ADMIN_SESSION_TTL_SECONDS
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ConfigurationError(
            "RECROOM_ADMIN_SESSION_TTL_SECONDS must be an integer."
        ) from exc
    if not 300 <= value <= 7 * 24 * 60 * 60:
        raise ConfigurationError(
            "RECROOM_ADMIN_SESSION_TTL_SECONDS must be between 300 and 604800."
        )
    return value


def admin_secret_fingerprint(secret: str) -> str:
    return hashlib.sha256(("recroom-admin-secret:" + secret).encode("utf-8")).hexdigest()


def admin_session_token_hash(token: str) -> str:
    return hashlib.sha256(("recroom-admin-session:" + token).encode("utf-8")).hexdigest()


async def create_admin_session(
    transient: redis_state.RedisTransientState,
    *,
    operator_id: str,
    secret: str,
    ttl_seconds: int,
) -> tuple[str, dict[str, Any]]:
    token = secrets.token_urlsafe(48)
    csrf_token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=ttl_seconds)
    session = {
        "session_id": str(uuid.uuid4()),
        "operator_id": operator_id,
        "created_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "expires_at": expires_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "last_used_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "csrf_token": csrf_token,
    }
    session["secret_fingerprint"] = admin_secret_fingerprint(secret)
    await transient.create_admin_session(
        token_hash=admin_session_token_hash(token),
        session=session,
        ttl_seconds=ttl_seconds,
    )
    return token, session


async def get_admin_session(
    transient: redis_state.RedisTransientState,
    token: str,
    *,
    configured_secret: str,
    touch: bool = True,
) -> dict[str, Any] | None:
    if not token or len(token) > 512:
        return None
    now = datetime.now(timezone.utc)
    now_text = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    token_hash = admin_session_token_hash(token)
    session = await transient.get_admin_session(token_hash)
    if session is None:
        return None
    expected_fingerprint = admin_secret_fingerprint(configured_secret)
    if not hmac.compare_digest(
        str(session.get("secret_fingerprint") or ""),
        expected_fingerprint,
    ):
        await transient.revoke_admin_session(token_hash)
        return None
    try:
        expires_at = datetime.fromisoformat(
            str(session["expires_at"]).replace("Z", "+00:00")
        )
    except (KeyError, ValueError):
        await transient.revoke_admin_session(token_hash)
        return None
    if expires_at <= now:
        await transient.revoke_admin_session(token_hash)
        return None
    if touch:
        session["last_used_at"] = now_text
    return session


async def revoke_admin_session(
    transient: redis_state.RedisTransientState,
    token: str,
) -> None:
    if not token:
        return
    await transient.revoke_admin_session(admin_session_token_hash(token))


def admin_key_from_request(request: Request) -> str:
    value = str(request.headers.get("x-rec-room-admin-key") or request.headers.get("x-recroom-admin-key") or "").strip()
    if value:
        return value
    authorization = str(request.headers.get("authorization") or "").strip()
    if authorization.casefold().startswith("bearer "):
        return authorization[7:].strip()
    return ""


def require_admin_key(request: Request) -> None:
    expected = configured_admin_key()
    minimum_length = 124 if _is_railway_environment() else 64
    if not expected or len(expected) < minimum_length:
        raise HTTPException(status_code=503, detail="Admin API key is not configured.")
    provided = admin_key_from_request(request)
    if not provided:
        raise HTTPException(status_code=401, detail="Administrator authentication is required.")
    if not hmac.compare_digest(provided, expected):
        raise HTTPException(status_code=403, detail="Forbidden.")


def admin_api_version_from_payload(payload: dict[str, Any], *, default: str | None = None) -> str | None:
    api_version = str(payload.get("api_version") or payload.get("apiVersion") or default or "").strip()
    if not api_version:
        return None
    if not API_VERSION_RE.fullmatch(api_version):
        raise HTTPException(status_code=400, detail="Invalid api_version.")
    return api_version


def payload_truthy(payload: dict[str, Any], *names: str) -> bool:
    for name in names:
        if name not in payload:
            continue
        value = payload[name]
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        return str(value).strip().casefold() in {"1", "true", "yes", "y", "on"}
    return False


def append_recnet_identity_pairs(identities: list[tuple[str, Any]], recnet_id: str, api_version: str | None) -> None:
    if not recnet_id:
        return
    identities.append(("account_id", f"recnet:{recnet_id}"))
    if api_version:
        identities.append(("account_id", f"{api_version}:recnet:{recnet_id}"))
        identities.append(("account_id", f"local-{api_version}-{recnet_id}"))


def get_or_create_player(
    db: Database,
    *,
    api_version: str,
    identity_key: str | None = None,
    username: str | None = None,
    display_name: str | None = None,
) -> dict[str, Any]:
    state_identity = identity_key or username or display_name
    now = utc_now()
    with db.transaction() as conn:
        row = None
        if username:
            row = conn.execute("SELECT * FROM players WHERE username = ?", (username,)).fetchone()
        if row is None and state_identity:
            state_row = conn.execute(
                """
                SELECT p.*
                FROM player_version_state AS pvs
                JOIN players AS p ON p.player_id = pvs.player_id
                WHERE pvs.api_version = ? AND json_extract(pvs.state_json, '$.identity_key') = ?
                """,
                (api_version, state_identity),
            ).fetchone()
            row = state_row

        if row is None:
            player_id = str(uuid.uuid4())
            username = username or f"Player_{secrets.token_hex(4)}"
            display_name = display_name or username
            conn.execute(
                """
                INSERT INTO players (
                    player_id, username, display_name, email, verified, permissions_json,
                    canonical_level, canonical_xp, profile_picture_asset_id, is_coach,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    player_id,
                    username,
                    display_name,
                    DEFAULT_CREATED_PLAYER_EMAIL,
                    1,
                    json.dumps(DEV_PERMISSIONS),
                    1,
                    0,
                    None,
                    0,
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO player_version_state(player_id, api_version, state_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (player_id, api_version, json.dumps({"identity_key": state_identity}), now, now),
            )
            row = conn.execute("SELECT * FROM players WHERE player_id = ?", (player_id,)).fetchone()
        else:
            conn.execute(
                """
                INSERT INTO player_version_state(player_id, api_version, state_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(player_id, api_version) DO NOTHING
                """,
                (row["player_id"], api_version, json.dumps({"identity_key": state_identity}), now, now),
            )
    return row_to_canonical_player(row)


class ServerContext:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        content_filter: ContentFilter | None = None,
        transient: redis_state.RedisTransientState | None = None,
    ):
        self.settings = settings
        self.db = db
        self.content_filter = content_filter
        self.transient = transient

    def require_transient(self) -> redis_state.RedisTransientState:
        if self.transient is None:
            raise HTTPException(
                status_code=503,
                detail="Shared transient state is unavailable.",
            )
        return self.transient

    @property
    def data_dir(self) -> Path:
        return self.settings.data_dir

    @property
    def db_path(self) -> Path:
        return self.settings.db_path

    def record_endpoint_error_alert(
        self,
        *,
        method: str,
        route_path: str,
        api_version: str,
        adapter_file: str,
        status_code: int,
        request_summary: dict[str, Any],
        error_detail: str,
    ) -> dict[str, Any]:
        now = utc_now()
        normalized_route_path = _normalize_alert_route_path(route_path)
        endpoint_key = endpoint_alert_key(method, normalized_route_path)
        method = method.upper()
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM endpoint_error_alerts WHERE endpoint_key = ?",
                (endpoint_key,),
            ).fetchone()
            if row is None:
                versions = [api_version]
                conn.execute(
                    """
                    INSERT INTO endpoint_error_alerts(
                        endpoint_key, method, route_path, api_versions_json, latest_api_version,
                        latest_adapter_file, latest_status_code, request_count, webhook_message_id,
                        first_seen_at, last_seen_at, last_request_summary_json, last_error_detail
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?, ?, ?)
                    """,
                    (
                        endpoint_key,
                        method,
                        normalized_route_path,
                        json.dumps(versions),
                        api_version,
                        adapter_file,
                        status_code,
                        now,
                        now,
                        json.dumps(request_summary, sort_keys=True),
                        error_detail,
                    ),
                )
                request_count = 1
                message_id = None
                is_new = True
            else:
                try:
                    versions = json.loads(row["api_versions_json"])
                except Exception:
                    versions = []
                request_count = int(row["request_count"])
                message_id = row["webhook_message_id"]
                is_new = False
        return {
            "endpoint_key": endpoint_key,
            "method": method,
            "route_path": normalized_route_path,
            "api_versions": versions,
            "latest_api_version": api_version,
            "latest_adapter_file": adapter_file,
            "latest_status_code": status_code,
            "request_count": request_count,
            "webhook_message_id": message_id,
            "first_seen_at": now if is_new else row["first_seen_at"],
            "last_seen_at": now,
            "last_request_summary": request_summary,
            "last_error_detail": error_detail,
            "is_new": is_new,
        }

    def set_endpoint_error_alert_message_id(self, endpoint_key: str, message_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE endpoint_error_alerts SET webhook_message_id = ? WHERE endpoint_key = ?",
                (message_id, endpoint_key),
            )

    def get_server_setting(self, setting_key: str, default: Any = None) -> Any:
        with self.db.connection() as conn:
            row = conn.execute("SELECT value_json FROM server_settings WHERE key = ?", (setting_key,)).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value_json"])
        except Exception:
            return row["value_json"]

    def public_api_origin(self, request: Request, api_version: str) -> str:
        return public_api_origin(request, self, api_version)

    def public_api_base_url(self, request: Request, api_version: str) -> str:
        return public_api_base_url(request, self, api_version)

    def public_websocket_origin(self, request: Request, api_version: str) -> str:
        return public_websocket_origin(request, self, api_version)

    def public_websocket_base_url(self, request: Request, api_version: str) -> str:
        return public_websocket_base_url(request, self, api_version)

    def get_motd(self, api_version: str | None = None) -> str:
        """Return the shared canonical MOTD.

        api_version is accepted so version adapters can call the same helper,
        but it does not select a separate live MOTD key. The build-specific
        files only decide how to serialize the shared text.
        """
        value = self.get_server_setting("motd", "")
        return value if isinstance(value, str) else ""

    @staticmethod
    def _decode_setting_string(row: sqlite3.Row | None) -> str | None:
        if row is None:
            return None
        raw_value = row["value_json"]
        if not isinstance(raw_value, str) or raw_value == "":
            return None
        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError:
            value = raw_value
        return value if isinstance(value, str) else None

    def set_shared_motd(self, message: str) -> None:
        self._set_server_setting("motd", message)

    def set_motd(self, api_version: str, message: str) -> None:
        # MOTD is a shared canonical server setting for the currently
        # supported builds. Keep this method for older adapter/admin callers,
        # but do not create build-local MOTD keys.
        self.set_shared_motd(message)

    def _set_server_setting(self, setting_key: str, value: Any) -> None:
        now = utc_now()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO server_settings(key, value_json, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
                """,
                (setting_key, json.dumps(value), now, now),
            )

    def filter_user_text(
        self,
        value: str,
        *,
        policy: str,
        field_context: str,
        player_id: str | None = None,
        source_version: str | None = None,
    ) -> FilterResult:
        if self.content_filter is None:
            raise ContentFilterError("The canonical content filter is not initialized.")
        try:
            result = self.content_filter.apply(
                value,
                policy=policy,
                context=field_context,
            )
        except ProhibitedProfileText:
            with self.db.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO content_filter_events(
                        event_id, player_id, source_version, field_context, policy,
                        changed, blocked, match_count, list_version, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, 0, 1, 1, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        player_id,
                        source_version,
                        field_context,
                        policy,
                        self.content_filter.list_version,
                        utc_now(),
                    ),
                )
            raise
        if result.changed or result.blocked:
            with self.db.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO content_filter_events(
                        event_id, player_id, source_version, field_context, policy,
                        changed, blocked, match_count, list_version, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        player_id,
                        source_version,
                        field_context,
                        policy,
                        int(result.changed),
                        int(result.blocked),
                        len(result.matched_term_ids),
                        result.list_version,
                        utc_now(),
                    ),
                )
        return result

    def create_moderation_report(self, **kwargs: Any) -> dict[str, Any]:
        return moderation_service.create_report(self.db, **kwargs)

    def get_maintenance_state(self, *, now: datetime | None = None) -> dict[str, Any]:
        with self.db.connection() as conn:
            row = conn.execute(
                "SELECT * FROM maintenance_state WHERE singleton_id = 1"
            ).fetchone()
        if row is None:
            return {
                "active": False,
                "starts_at_utc": None,
                "starts_in_minutes": 0,
                "revision": 0,
                "updated_at": None,
                "updated_by": None,
            }
        state = {
            "active": bool(row["active"]),
            "starts_at_utc": row["starts_at_utc"],
            "revision": int(row["revision"]),
            "updated_at": row["updated_at"],
            "updated_by": row["updated_by"],
        }
        state["starts_in_minutes"] = (
            maintenance_remaining_minutes(state["starts_at_utc"], now=now)
            if state["active"]
            else 0
        )
        return state

    def schedule_maintenance(
        self,
        *,
        starts_in_minutes: int,
        actor_id: str,
        reason: str | None,
        idempotency_key: str | None,
        now: datetime | None = None,
    ) -> tuple[dict[str, Any], bool]:
        now = (now or utc_datetime_now()).astimezone(timezone.utc)
        starts_at = now + timedelta(minutes=starts_in_minutes)
        now_text = utc_datetime_text(now)
        starts_at_text = utc_datetime_text(starts_at)
        with self.db.transaction() as conn:
            if idempotency_key:
                duplicate = conn.execute(
                    """
                    SELECT audit_id
                    FROM maintenance_audit
                    WHERE idempotency_key = ?
                    """,
                    (idempotency_key,),
                ).fetchone()
                if duplicate is not None:
                    current_row = conn.execute(
                        "SELECT * FROM maintenance_state WHERE singleton_id = 1"
                    ).fetchone()
                    if current_row is None:
                        return {
                            "active": False,
                            "starts_at_utc": None,
                            "starts_in_minutes": 0,
                            "revision": 0,
                            "updated_at": None,
                            "updated_by": None,
                        }, False
                    replayed_state = {
                        "active": bool(current_row["active"]),
                        "starts_at_utc": current_row["starts_at_utc"],
                        "revision": int(current_row["revision"]),
                        "updated_at": current_row["updated_at"],
                        "updated_by": current_row["updated_by"],
                    }
                    replayed_state["starts_in_minutes"] = (
                        maintenance_remaining_minutes(
                            replayed_state["starts_at_utc"],
                            now=now,
                        )
                        if replayed_state["active"]
                        else 0
                    )
                    return replayed_state, False
            previous_row = conn.execute(
                "SELECT * FROM maintenance_state WHERE singleton_id = 1"
            ).fetchone()
            previous = dict(previous_row) if previous_row is not None else {
                "singleton_id": 1,
                "active": 0,
                "starts_at_utc": None,
                "revision": 0,
                "updated_at": None,
                "updated_by": None,
            }
            revision = int(previous["revision"]) + 1
            conn.execute(
                """
                INSERT INTO maintenance_state(
                    singleton_id, active, starts_at_utc, revision, updated_at, updated_by
                )
                VALUES (1, 1, ?, ?, ?, ?)
                ON CONFLICT(singleton_id) DO UPDATE SET
                    active = excluded.active,
                    starts_at_utc = excluded.starts_at_utc,
                    revision = excluded.revision,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
                """,
                (starts_at_text, revision, now_text, actor_id),
            )
            current = {
                "singleton_id": 1,
                "active": 1,
                "starts_at_utc": starts_at_text,
                "revision": revision,
                "updated_at": now_text,
                "updated_by": actor_id,
            }
            conn.execute(
                """
                INSERT INTO maintenance_audit(
                    audit_id, action, revision, actor_id, reason, idempotency_key,
                    previous_state_json, new_state_json, created_at
                )
                VALUES (?, 'schedule', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    revision,
                    actor_id,
                    reason,
                    idempotency_key,
                    json.dumps(previous, sort_keys=True),
                    json.dumps(current, sort_keys=True),
                    now_text,
                ),
            )
        return self.get_maintenance_state(now=now), True

    def cancel_maintenance(
        self,
        *,
        actor_id: str,
        reason: str | None,
        idempotency_key: str | None,
        now: datetime | None = None,
    ) -> tuple[dict[str, Any], bool]:
        now = (now or utc_datetime_now()).astimezone(timezone.utc)
        now_text = utc_datetime_text(now)
        with self.db.transaction() as conn:
            if idempotency_key:
                duplicate = conn.execute(
                    "SELECT audit_id FROM maintenance_audit WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if duplicate is not None:
                    current_row = conn.execute(
                        "SELECT * FROM maintenance_state WHERE singleton_id = 1"
                    ).fetchone()
                    if current_row is None:
                        return {
                            "active": False,
                            "starts_at_utc": None,
                            "starts_in_minutes": 0,
                            "revision": 0,
                            "updated_at": None,
                            "updated_by": None,
                        }, False
                    replayed_state = {
                        "active": bool(current_row["active"]),
                        "starts_at_utc": current_row["starts_at_utc"],
                        "revision": int(current_row["revision"]),
                        "updated_at": current_row["updated_at"],
                        "updated_by": current_row["updated_by"],
                    }
                    replayed_state["starts_in_minutes"] = (
                        maintenance_remaining_minutes(
                            replayed_state["starts_at_utc"],
                            now=now,
                        )
                        if replayed_state["active"]
                        else 0
                    )
                    return replayed_state, False
            previous_row = conn.execute(
                "SELECT * FROM maintenance_state WHERE singleton_id = 1"
            ).fetchone()
            previous = dict(previous_row) if previous_row is not None else {
                "singleton_id": 1,
                "active": 0,
                "starts_at_utc": None,
                "revision": 0,
                "updated_at": None,
                "updated_by": None,
            }
            revision = int(previous["revision"]) + 1
            conn.execute(
                """
                INSERT INTO maintenance_state(
                    singleton_id, active, starts_at_utc, revision, updated_at, updated_by
                )
                VALUES (1, 0, NULL, ?, ?, ?)
                ON CONFLICT(singleton_id) DO UPDATE SET
                    active = 0,
                    starts_at_utc = NULL,
                    revision = excluded.revision,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
                """,
                (revision, now_text, actor_id),
            )
            current = {
                "singleton_id": 1,
                "active": 0,
                "starts_at_utc": None,
                "revision": revision,
                "updated_at": now_text,
                "updated_by": actor_id,
            }
            conn.execute(
                """
                INSERT INTO maintenance_audit(
                    audit_id, action, revision, actor_id, reason, idempotency_key,
                    previous_state_json, new_state_json, created_at
                )
                VALUES (?, 'cancel', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    revision,
                    actor_id,
                    reason,
                    idempotency_key,
                    json.dumps(previous, sort_keys=True),
                    json.dumps(current, sort_keys=True),
                    now_text,
                ),
            )
        return self.get_maintenance_state(now=now), True

    def ensure_anchored_schedule(self, **kwargs: Any) -> dict[str, Any]:
        return timed_content.ensure_anchored_schedule(self.db, **kwargs)

    def reconcile_anchored_period(self, **kwargs: Any) -> dict[str, Any]:
        return timed_content.reconcile_anchored_period(self.db, **kwargs)

    def reconcile_registered_period(self, **kwargs: Any) -> dict[str, Any]:
        return timed_content.reconcile_registered_period(self.db, **kwargs)

    def is_player_administrator(self, player_id: Any) -> bool:
        canonical_player_id = str(player_id or "").strip()
        if not canonical_player_id:
            return False
        configured = self.get_server_setting(SERVER_ADMINISTRATOR_IDS_SETTING, [])
        if not isinstance(configured, list):
            return False
        return canonical_player_id in {
            str(value).strip() for value in configured if str(value).strip()
        }

    def set_player_administrator(self, player_id: Any, administrator: bool) -> bool:
        canonical_player_id = str(player_id or "").strip()
        if not canonical_player_id:
            raise ValueError("A canonical player ID is required.")
        now = utc_now()
        with self.db.transaction() as conn:
            player_exists = conn.execute(
                "SELECT 1 FROM players WHERE player_id = ?",
                (canonical_player_id,),
            ).fetchone()
            if player_exists is None:
                raise ValueError("Player does not exist.")
            row = conn.execute(
                "SELECT value_json FROM server_settings WHERE key = ?",
                (SERVER_ADMINISTRATOR_IDS_SETTING,),
            ).fetchone()
            try:
                configured = json.loads(row["value_json"]) if row is not None else []
            except Exception:
                configured = []
            if not isinstance(configured, list):
                configured = []
            administrator_ids = {
                str(value).strip() for value in configured if str(value).strip()
            }
            if administrator:
                administrator_ids.add(canonical_player_id)
            else:
                administrator_ids.discard(canonical_player_id)
            conn.execute(
                """
                INSERT INTO server_settings(key, value_json, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (
                    SERVER_ADMINISTRATOR_IDS_SETTING,
                    json.dumps(sorted(administrator_ids)),
                    now,
                    now,
                ),
            )
        return administrator

    def find_player_by_identity(self, identity_type: str, value: Any) -> dict[str, Any] | None:
        identity_hash = self.identity_hash(identity_type, value)
        if not identity_hash:
            return None
        with self.db.connection() as conn:
            row = conn.execute(
                """
                SELECT p.*
                FROM player_identities AS pi
                JOIN players AS p ON p.player_id = pi.player_id
                WHERE pi.identity_type = ?
                  AND pi.identity_hash = ?
                ORDER BY pi.last_seen_at DESC
                LIMIT 1
                """,
                (identity_type, identity_hash),
            ).fetchone()
        return row_to_canonical_player(row) if row else None

    def ensure_player_version_state(
        self,
        player_id: str,
        api_version: str,
        default_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        default_state = dict(default_state or {})
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT state_json FROM player_version_state WHERE player_id = ? AND api_version = ?",
                (player_id, api_version),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO player_version_state(player_id, api_version, state_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (player_id, api_version, json.dumps(default_state, sort_keys=True), now, now),
                )
                return default_state
            try:
                state = json.loads(row["state_json"] or "{}")
            except Exception:
                state = {}
            return state if isinstance(state, dict) else {}

    def get_or_create_player(self, api_version: str, **kwargs: Any) -> dict[str, Any]:
        username = kwargs.get("username")
        display_name = kwargs.get("display_name")
        identity_key = kwargs.get("identity_key")
        self.assert_identities_not_banned(
            [
                ("username_lower", username),
                ("username_lower", display_name),
                ("account_id", identity_key),
            ]
        )

        player = self.find_player_by_identity("account_id", identity_key) if identity_key else None
        if player is not None:
            self.ensure_player_version_state(player["player_id"], api_version, {"identity_key": identity_key})
        else:
            player = get_or_create_player(self.db, api_version=api_version, **kwargs)

        identities = [
            ("account_id", player["player_id"]),
            ("username_lower", player["username"]),
            ("username_lower", player["display_name"]),
            ("account_id", identity_key),
        ]
        self.record_player_identities(player["player_id"], identities)
        self.assert_player_not_banned(player["player_id"])
        return player

    def insert_bug_report(
        self,
        conn: sqlite3.Connection,
        **report: Any,
    ) -> dict[str, Any]:
        """Expose the canonical bug-report store to dated API adapters."""
        return insert_bug_report(conn, **report)

    def identity_hash(self, identity_type: str, value: Any) -> str:
        return hash_ban_identity(self.settings.ban_hash_pepper, identity_type, value)

    def request_ip_value(self, request_or_websocket: Any) -> str:
        headers = getattr(request_or_websocket, "headers", {})
        forwarded_for = str(headers.get("x-forwarded-for") or "").split(",", 1)[0].strip()
        if forwarded_for:
            return forwarded_for
        client = getattr(request_or_websocket, "client", None)
        return str(getattr(client, "host", "") or "")

    def request_identity_pairs(self, request_or_websocket: Any, api_version: str) -> list[tuple[str, Any]]:
        headers = getattr(request_or_websocket, "headers", {})
        pairs: list[tuple[str, Any]] = [("ip_hash", self.request_ip_value(request_or_websocket))]
        profile_id = str(headers.get("x-rec-room-profile") or "").strip()
        if profile_id:
            pairs.append(("account_id", f"recnet:{profile_id}"))
            pairs.append(("account_id", f"{api_version}:recnet:{profile_id}"))
        authorization = str(headers.get("authorization") or "").strip()
        if authorization.casefold().startswith("bearer "):
            authorization = authorization[7:].strip()
        if authorization:
            token_prefix = f"local-{api_version}-"
            if authorization.casefold().startswith(token_prefix.casefold()):
                recnet_id = authorization[len(token_prefix) :].strip()
                if recnet_id:
                    pairs.append(("account_id", f"recnet:{recnet_id}"))
                    pairs.append(("account_id", f"{api_version}:recnet:{recnet_id}"))
        player = self.player_from_request(request_or_websocket, api_version)
        if player:
            pairs.append(("account_id", player["player_id"]))
            pairs.append(("username_lower", player["username"]))
            pairs.append(("username_lower", player["display_name"]))
            try:
                state = json.loads(player["state_json"])
            except Exception:
                state = {}
            recnet_id = state.get("recnet_id")
            if recnet_id:
                pairs.append(("account_id", f"recnet:{recnet_id}"))
                pairs.append(("account_id", f"{api_version}:recnet:{recnet_id}"))
            platform_id = str(state.get("platform_id") or "").strip()
            if platform_id:
                try:
                    platform = int(state.get("platform", 0) or 0)
                except (TypeError, ValueError):
                    platform = 0
                if platform == 0:
                    pairs.append(("account_id", f"steam:{platform_id}"))
                pairs.append(("account_id", f"platform:{platform}:{platform_id}"))
                pairs.append(("account_id", f"{api_version}:platform:{platform}:{platform_id}"))
        return pairs

    async def issue_player_session(
        self,
        *,
        api_version: str,
        raw_token: str,
        player_id: str,
        legacy_player_id: int | None = None,
        ttl_seconds: int = 24 * 60 * 60,
        session_type: str = "access",
    ) -> None:
        token_hash = hashlib.sha256(
            ("recroom-player-session:" + raw_token).encode("utf-8")
        ).hexdigest()
        await self.require_transient().create_player_session(
            token_hash=token_hash,
            player_id=player_id,
            session={
                "api_version": api_version,
                "player_id": player_id,
                "legacy_player_id": legacy_player_id,
                "session_type": session_type,
                "issued_at": utc_now(),
            },
            ttl_seconds=ttl_seconds,
        )

    async def revoke_request_player_session(self, request_or_websocket: Any) -> None:
        headers = getattr(request_or_websocket, "headers", {})
        authorization = str(headers.get("authorization") or "").strip()
        if not authorization.casefold().startswith("bearer "):
            return
        raw_token = authorization[7:].strip()
        if not raw_token or len(raw_token) > 8192:
            return
        token_hash = hashlib.sha256(
            ("recroom-player-session:" + raw_token).encode("utf-8")
        ).hexdigest()
        await self.require_transient().revoke_player_session(token_hash)

    def transient_player_aliases(self, player_id: str) -> list[str]:
        aliases: list[str] = []
        with self.db.connection() as conn:
            rows = conn.execute(
                "SELECT state_json FROM player_version_state WHERE player_id = ?",
                (player_id,),
            ).fetchall()
        for row in rows:
            try:
                state = json.loads(row["state_json"] or "{}")
            except (TypeError, ValueError):
                continue
            if not isinstance(state, dict):
                continue
            for key in ("legacy_player_id", "recnet_id"):
                value = str(state.get(key) or "").strip()
                if value and value not in aliases:
                    aliases.append(value)
        return aliases

    async def resolve_request_player_session(
        self,
        request_or_websocket: Any,
        api_version: str,
    ) -> dict[str, Any] | None:
        headers = getattr(request_or_websocket, "headers", {})
        authorization = str(headers.get("authorization") or "").strip()
        if not authorization.casefold().startswith("bearer "):
            return None
        raw_token = authorization[7:].strip()
        if not raw_token or len(raw_token) > 8192:
            return None
        session = await self.player_session_for_token(raw_token, api_version)
        state = getattr(request_or_websocket, "state", None)
        if state is not None:
            state.redis_player_session = session
        return session

    async def player_session_for_token(
        self, raw_token: str, api_version: str
    ) -> dict[str, Any] | None:
        if not raw_token or len(raw_token) > 8192:
            return None
        token_hash = hashlib.sha256(
            ("recroom-player-session:" + raw_token).encode("utf-8")
        ).hexdigest()
        session = await self.require_transient().get_json(
            "player-session", token_hash
        )
        if not isinstance(session, dict) or str(session.get("api_version")) != api_version:
            return None
        return session

    def record_player_identities(self, player_id: str, identities: list[tuple[str, Any]]) -> None:
        now = utc_now()
        with self.db.transaction() as conn:
            for identity_type, value in identities:
                identity_hash = self.identity_hash(identity_type, value)
                if not identity_hash:
                    continue
                conn.execute(
                    """
                    INSERT INTO player_identities(
                        player_id, identity_type, identity_hash, first_seen_at, last_seen_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(identity_type, identity_hash, player_id)
                    DO UPDATE SET last_seen_at = excluded.last_seen_at
                    """,
                    (player_id, identity_type, identity_hash, now, now),
                )

    def remember_request_identities(self, player_id: str, request_or_websocket: Any, api_version: str) -> None:
        self.record_player_identities(player_id, self.request_identity_pairs(request_or_websocket, api_version))

    def active_ban_for_identities(self, identities: list[tuple[str, Any]]) -> sqlite3.Row | None:
        with self.db.connection() as conn:
            for identity_type, value in identities:
                identity_hash = self.identity_hash(identity_type, value)
                if not identity_hash:
                    continue
                row = conn.execute(
                    """
                    SELECT *
                    FROM bans
                    WHERE identity_type = ?
                      AND identity_hash = ?
                      AND active = 1
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (identity_type, identity_hash),
                ).fetchone()
                if row:
                    return row
        return None

    def assert_identities_not_banned(self, identities: list[tuple[str, Any]]) -> None:
        ban = self.active_ban_for_identities(identities)
        if ban:
            raise HTTPException(status_code=403, detail=ban["reason"] or "This account is banned.")

    def active_sanction_for_identities(
        self,
        identities: list[tuple[str, Any]],
        *,
        include_network: bool = False,
    ) -> sqlite3.Row | None:
        allowed_types = {"account_id"}
        if include_network:
            allowed_types.add("ip_hash")
        now = utc_now()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE moderation_sanctions
                SET active = 0, updated_at = ?
                WHERE active = 1
                  AND expires_at IS NOT NULL
                  AND expires_at <= ?
                """,
                (now, now),
            )
            for identity_type, value in identities:
                if identity_type not in allowed_types:
                    continue
                identity_hash = self.identity_hash(identity_type, value)
                if not identity_hash:
                    continue
                row = conn.execute(
                    """
                    SELECT ms.*
                    FROM player_identities AS pi
                    JOIN moderation_sanctions AS ms
                      ON ms.target_player_id = pi.player_id
                    WHERE pi.identity_type = ?
                      AND pi.identity_hash = ?
                      AND ms.active = 1
                      AND ms.scope = 'account'
                      AND ms.starts_at <= ?
                      AND (ms.expires_at IS NULL OR ms.expires_at > ?)
                    ORDER BY
                        CASE ms.sanction_type
                            WHEN 'ban' THEN 0
                            WHEN 'timeout' THEN 1
                            ELSE 2
                        END,
                        ms.created_at DESC
                    LIMIT 1
                    """,
                    (identity_type, identity_hash, now, now),
                ).fetchone()
                if row:
                    return row
        return None

    def assert_identities_not_sanctioned(
        self,
        identities: list[tuple[str, Any]],
        *,
        include_network: bool = False,
    ) -> None:
        sanction = self.active_sanction_for_identities(
            identities,
            include_network=include_network,
        )
        if sanction is None:
            return
        sanction_type = str(sanction["sanction_type"] or "restriction")
        raise HTTPException(
            status_code=403,
            detail=str(sanction["reason"] or f"This account has an active {sanction_type}."),
        )

    def assert_account_creation_allowed(
        self,
        request_or_websocket: Any,
        api_version: str,
        identities: list[tuple[str, Any]],
    ) -> None:
        combined = list(identities)
        combined.extend(self.request_identity_pairs(request_or_websocket, api_version))
        self.assert_identities_not_banned(combined)
        # Account access is linked only through durable account/platform
        # identities. During account creation there may not be one yet, so the
        # request network is a narrow fallback that prevents creating an
        # immediate throwaway profile to evade an active timeout. It never
        # creates a ban and stops applying when the original timeout expires.
        self.assert_identities_not_sanctioned(combined, include_network=True)

    def assert_request_not_banned(self, request_or_websocket: Any, api_version: str) -> None:
        identities = self.request_identity_pairs(request_or_websocket, api_version)
        self.assert_identities_not_banned(identities)
        player = self.player_from_request(request_or_websocket, api_version)
        if player:
            self.remember_request_identities(player["player_id"], request_or_websocket, api_version)
            self.assert_player_not_banned(player["player_id"])

    def player_from_request(self, request_or_websocket: Any, api_version: str) -> sqlite3.Row | None:
        headers = getattr(request_or_websocket, "headers", {})
        recnet_id = str(headers.get("x-rec-room-profile") or "").strip()
        authorization = str(headers.get("authorization") or "").strip()
        if authorization.casefold().startswith("bearer "):
            authorization = authorization[7:].strip()
        session = getattr(
            getattr(request_or_websocket, "state", None),
            "redis_player_session",
            None,
        )
        session_player_id = (
            str(session.get("player_id") or "")
            if isinstance(session, dict)
            and str(session.get("api_version") or "") == api_version
            else ""
        )
        token_prefix = f"local-{api_version}-"
        if not recnet_id and authorization.casefold().startswith(token_prefix.casefold()):
            recnet_id = authorization[len(token_prefix) :].strip()
        with self.db.connection() as conn:
            if authorization:
                if not session_player_id:
                    return None
                return conn.execute(
                    """
                    SELECT p.*, pvs.state_json
                    FROM players p
                    JOIN player_version_state pvs ON p.player_id = pvs.player_id
                    WHERE p.player_id = ? AND pvs.api_version = ?
                    """,
                    (session_player_id, api_version),
                ).fetchone()
            if recnet_id:
                try:
                    recnet_id_value = int(recnet_id)
                except ValueError:
                    recnet_id_value = 0
                if recnet_id_value > 0:
                    row = conn.execute(
                        """
                        SELECT p.*, pvs.state_json
                        FROM players p
                        JOIN player_version_state pvs ON p.player_id = pvs.player_id
                        WHERE pvs.api_version = ?
                          AND json_extract(pvs.state_json, '$.recnet_id') = ?
                        """,
                        (api_version, recnet_id_value),
                    ).fetchone()
                    if row:
                        return row
        return None

    def assert_player_not_banned(self, player_id: str) -> None:
        with self.db.connection() as conn:
            row = conn.execute("SELECT * FROM players WHERE player_id = ?", (player_id,)).fetchone()
        if row and bool(row["is_banned"]):
            self.enforce_ban_cleanup(player_id)
            raise HTTPException(status_code=403, detail=row["ban_reason"] or "This account is banned.")
        sanction = moderation_service.active_player_sanction(self.db, player_id)
        if sanction is None:
            with self.db.connection() as conn:
                identity_rows = conn.execute(
                    """
                    SELECT identity_type, identity_hash
                    FROM player_identities
                    WHERE player_id = ?
                      AND identity_type = 'account_id'
                    """,
                    (player_id,),
                ).fetchall()
            now = utc_now()
            with self.db.transaction() as conn:
                conn.execute(
                    """
                    UPDATE moderation_sanctions
                    SET active = 0, updated_at = ?
                    WHERE active = 1
                      AND expires_at IS NOT NULL
                      AND expires_at <= ?
                    """,
                    (now, now),
                )
                for identity in identity_rows:
                    sanction_row = conn.execute(
                        """
                        SELECT ms.*
                        FROM player_identities AS linked
                        JOIN moderation_sanctions AS ms
                          ON ms.target_player_id = linked.player_id
                        WHERE linked.identity_type = ?
                          AND linked.identity_hash = ?
                          AND ms.active = 1
                          AND ms.scope = 'account'
                          AND ms.starts_at <= ?
                          AND (ms.expires_at IS NULL OR ms.expires_at > ?)
                        ORDER BY
                            CASE ms.sanction_type
                                WHEN 'ban' THEN 0
                                WHEN 'timeout' THEN 1
                                ELSE 2
                            END,
                            ms.created_at DESC
                        LIMIT 1
                        """,
                        (
                            identity["identity_type"],
                            identity["identity_hash"],
                            now,
                            now,
                        ),
                    ).fetchone()
                    if sanction_row:
                        sanction = dict(sanction_row)
                        break
        if sanction is not None:
            sanction_type = str(sanction.get("sanction_type") or "restriction")
            raise HTTPException(
                status_code=403,
                detail=str(sanction.get("reason") or f"This account has an active {sanction_type}."),
            )

    def active_player_sanction(self, player_id: str) -> dict[str, Any] | None:
        return moderation_service.active_player_sanction(self.db, player_id)

    def has_player_restriction(self, player_id: str, scope: str) -> bool:
        return moderation_service.has_active_player_scope(
            self.db,
            player_id,
            scope,
        )

    def is_content_quarantined(self, target_type: str, target_id: Any) -> bool:
        return moderation_service.is_content_control_active(
            self.db,
            target_type=str(target_type),
            target_id=str(target_id),
            control_type="quarantine",
        )

    def enforce_ban_cleanup(self, player_id: str) -> None:
        """Revoke shared sessions while preserving dated-client identity state.

        Bans are access-control decisions, not account purges. Profile images,
        inventory, rooms, inventions, ownership records, and version state
        remain intact and reversible. Live bearer/admin sessions, connection
        leases, presence, and membership are revoked through Redis; the
        dispatcher also rejects sanctioned requests that were already in
        flight when revocation began.
        """
        with self.db.connection() as conn:
            player = conn.execute(
                "SELECT username, display_name, ban_reason FROM players WHERE player_id = ?",
                (player_id,),
            ).fetchone()
            identities = conn.execute(
                "SELECT identity_type, identity_hash FROM player_identities WHERE player_id = ?",
                (player_id,),
            ).fetchall()
        self.record_player_identities(
            player_id,
            [
                ("account_id", player_id),
                ("username_lower", player["username"] if player else ""),
                ("username_lower", player["display_name"] if player else ""),
            ],
        )
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM sessions WHERE player_id = ?", (player_id,))
            now = utc_now()
            refreshed_identities = conn.execute(
                "SELECT identity_type, identity_hash FROM player_identities WHERE player_id = ?",
                (player_id,),
            ).fetchall()
            for identity in list(identities) + list(refreshed_identities):
                # Account bans follow account/login identities. Network and
                # hardware restrictions are separate explicit operator
                # decisions; inheriting them here would punish shared NATs or
                # devices merely because one account was banned.
                if identity["identity_type"] not in {"account_id", "username_lower"}:
                    continue
                conn.execute(
                    """
                    INSERT INTO bans(id, player_id, identity_type, identity_hash, reason, active, created_at, updated_at)
                    SELECT ?, ?, ?, ?, ?, 1, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM bans
                        WHERE identity_type = ?
                          AND identity_hash = ?
                          AND active = 1
                    )
                    """,
                    (
                        str(uuid.uuid4()),
                        player_id,
                        identity["identity_type"],
                        identity["identity_hash"],
                        player["ban_reason"] if player else None,
                        now,
                        now,
                        identity["identity_type"],
                        identity["identity_hash"],
                    ),
                )

        # Redis owns live sessions, presence, membership, and connection
        # routing. The durable cleanup above remains reversible account/audit
        # work; transient revocation is scheduled on the active event loop and
        # never falls back to SQLite when Redis is unavailable.
        if self.transient is not None:
            try:
                asyncio.get_running_loop().create_task(
                    self.transient.revoke_player_transient_state(
                        player_id,
                        aliases=self.transient_player_aliases(player_id),
                    )
                )
            except RuntimeError:
                pass

    def create_player_ban(
        self,
        player_id: str,
        *,
        reason: str | None = None,
        extra_identities: list[tuple[str, Any]] | None = None,
        created_by: str = "operator",
        case_id: str | None = None,
    ) -> None:
        now = utc_now()
        identities = [
            (identity_type, value)
            for identity_type, value in (extra_identities or [])
            if identity_type in {"account_id", "username_lower"}
        ]
        with self.db.connection() as conn:
            row = conn.execute(
                "SELECT username, display_name, is_coach FROM players WHERE player_id = ?",
                (player_id,),
            ).fetchone()
            existing = conn.execute(
                "SELECT identity_type, identity_hash FROM player_identities WHERE player_id = ?",
                (player_id,),
            ).fetchall()
        if row is None:
            raise ValueError("Player does not exist.")
        if bool(row["is_coach"]):
            raise ValueError("Coach cannot be banned.")
        if row:
            identities.extend(
                [
                    ("account_id", player_id),
                    ("username_lower", row["username"]),
                    ("username_lower", row["display_name"]),
                ]
            )
        self.record_player_identities(player_id, identities)
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE players
                SET is_banned = 1, banned_at = COALESCE(banned_at, ?), ban_reason = ?, updated_at = ?
                WHERE player_id = ? AND is_coach = 0
                """,
                (now, reason, now, player_id),
            )
            active_sanction = conn.execute(
                """
                SELECT sanction_id
                FROM moderation_sanctions
                WHERE target_player_id = ?
                  AND sanction_type = 'ban'
                  AND active = 1
                LIMIT 1
                """,
                (player_id,),
            ).fetchone()
            if active_sanction is None:
                conn.execute(
                    """
                    INSERT INTO moderation_sanctions(
                        sanction_id, case_id, target_player_id, sanction_type, scope,
                        active, starts_at, expires_at, reason, created_by,
                        reversed_by_action_id, created_at, updated_at
                    )
                    VALUES (?, ?, ?, 'ban', 'account', 1, ?, NULL, ?, ?, NULL, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        case_id,
                        player_id,
                        now,
                        reason or "Banned by server operator.",
                        created_by,
                        now,
                        now,
                    ),
                )
            for identity_type, value in identities:
                identity_hash = self.identity_hash(identity_type, value)
                if identity_hash:
                    conn.execute(
                        """
                        INSERT INTO bans(id, player_id, identity_type, identity_hash, reason, active, created_at, updated_at)
                        SELECT ?, ?, ?, ?, ?, 1, ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM bans
                            WHERE identity_type = ? AND identity_hash = ? AND active = 1
                        )
                        """,
                        (
                            str(uuid.uuid4()), player_id, identity_type, identity_hash,
                            reason, now, now, identity_type, identity_hash,
                        ),
                    )
            for row in existing:
                if row["identity_type"] not in {"account_id", "username_lower"}:
                    continue
                conn.execute(
                    """
                    INSERT INTO bans(id, player_id, identity_type, identity_hash, reason, active, created_at, updated_at)
                    SELECT ?, ?, ?, ?, ?, 1, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM bans
                        WHERE identity_type = ? AND identity_hash = ? AND active = 1
                    )
                    """,
                    (
                        str(uuid.uuid4()), player_id, row["identity_type"], row["identity_hash"],
                        reason, now, now, row["identity_type"], row["identity_hash"],
                    ),
                )
        self.enforce_ban_cleanup(player_id)

    def create_identity_ban(
        self,
        identities: list[tuple[str, Any]],
        *,
        reason: str | None = None,
        player_id: str | None = None,
    ) -> int:
        now = utc_now()
        inserted = 0
        with self.db.transaction() as conn:
            for identity_type, value in identities:
                identity_hash = self.identity_hash(identity_type, value)
                if not identity_hash:
                    continue
                cursor = conn.execute(
                    """
                    INSERT INTO bans(id, player_id, identity_type, identity_hash, reason, active, created_at, updated_at)
                    SELECT ?, ?, ?, ?, ?, 1, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM bans
                        WHERE identity_type = ?
                          AND identity_hash = ?
                          AND active = 1
                    )
                    """,
                    (
                        str(uuid.uuid4()),
                        player_id,
                        identity_type,
                        identity_hash,
                        reason,
                        now,
                        now,
                        identity_type,
                        identity_hash,
                    ),
                )
                inserted += cursor.rowcount
        return inserted

    def unban_player(self, player_id: str) -> None:
        now = utc_now()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE players
                SET is_banned = 0,
                    banned_at = NULL,
                    ban_reason = NULL,
                    updated_at = ?
                WHERE player_id = ?
                """,
                (now, player_id),
            )
            conn.execute(
                """
                UPDATE moderation_sanctions
                SET active = 0, updated_at = ?
                WHERE target_player_id = ?
                  AND sanction_type IN ('ban', 'timeout')
                  AND active = 1
                """,
                (now, player_id),
            )
            conn.execute(
                """
                UPDATE bans
                SET active = 0,
                    updated_at = ?
                WHERE player_id = ?
                """,
                (now, player_id),
            )
            identity_rows = conn.execute(
                "SELECT identity_type, identity_hash FROM player_identities WHERE player_id = ?",
                (player_id,),
            ).fetchall()
            for row in identity_rows:
                conn.execute(
                    """
                    UPDATE bans
                    SET active = 0,
                        updated_at = ?
                    WHERE identity_type = ?
                      AND identity_hash = ?
                    """,
                    (now, row["identity_type"], row["identity_hash"]),
                )

    def unban_identities(self, identities: list[tuple[str, Any]]) -> int:
        now = utc_now()
        updated = 0
        with self.db.transaction() as conn:
            for identity_type, value in identities:
                identity_hash = self.identity_hash(identity_type, value)
                if not identity_hash:
                    continue
                cursor = conn.execute(
                    """
                    UPDATE bans
                    SET active = 0,
                        updated_at = ?
                    WHERE identity_type = ?
                      AND identity_hash = ?
                      AND active = 1
                    """,
                    (now, identity_type, identity_hash),
                )
                updated += cursor.rowcount
        return updated

    def save_image_bytes(
        self,
        *,
        owner_player_id: str | None,
        content: bytes,
        file_ext: str,
        mime_type: str,
        purpose: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ext = file_ext.lower()
        if ext not in {".png", ".jpg", ".jpeg"}:
            raise ValueError("Only .png, .jpg, and .jpeg image files are accepted.")
        guessed_ext = mimetypes.guess_extension(mime_type) or ext
        if mime_type not in {"image/png", "image/jpeg"} or guessed_ext.lower() not in {".png", ".jpg", ".jpeg"}:
            raise ValueError("Unsupported image MIME type.")

        asset_id = str(uuid.uuid4())
        filename = f"{asset_id}{ext}"
        bucket_name = PLAYER_IMAGE_DIR_NAME if owner_player_id else BACKEND_IMAGE_DIR_NAME
        path = validate_image_write_path(self.data_dir, filename, bucket_name)
        path.write_bytes(content)
        relative_path = f"{IMAGE_DATA_DIR_NAME}/{bucket_name}/{filename}"

        now = utc_now()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO data_assets(
                    asset_id, owner_player_id, relative_path, mime_type, file_ext,
                    purpose, metadata_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset_id,
                    owner_player_id,
                    relative_path,
                    mime_type,
                    ext,
                    purpose,
                    json.dumps(metadata or {}),
                    now,
                ),
            )
        return {
            "asset_id": asset_id,
            "relative_path": relative_path,
            "mime_type": mime_type,
            "file_ext": ext,
            "purpose": purpose,
            "metadata": metadata or {},
        }

    def find_image_path(self, filename: str) -> Path | None:
        clean_name = Path(filename).name
        if not clean_name or Path(clean_name).suffix.lower() not in ALLOWED_IMAGE_EXTENSIONS:
            return None
        image_dir = (self.data_dir / IMAGE_DATA_DIR_NAME).resolve()

        if image_dir.is_dir():
            for child in image_dir.rglob("*"):
                if child.is_file() and child.name.casefold() == clean_name.casefold():
                    return child.resolve()

        with self.db.connection() as conn:
            row = conn.execute(
                """
                SELECT relative_path FROM data_assets
                WHERE relative_path = ? OR relative_path = ? OR relative_path LIKE ?
                LIMIT 1
                """,
                (f"{IMAGE_DATA_DIR_NAME}/{clean_name}", clean_name, f"%/{clean_name}"),
            ).fetchone()
            if row:
                asset_path = (self.data_dir / str(row["relative_path"])).resolve()
                if asset_path.is_file() and self.data_dir.resolve() in asset_path.parents:
                    return asset_path
        return None

    def serve_image(self, filename: str) -> Response | None:
        path = self.find_image_path(filename)
        if path is None:
            return None
        ext = path.suffix.lower()
        mime_type = "image/png" if ext == ".png" else "image/jpeg"
        return Response(
            content=path.read_bytes(),
            media_type=mime_type,
            headers={
                "Cache-Control": "public, max-age=31536000, immutable",
            },
        )


class RateLimiter:
    """Redis-backed fixed-window limiter shared by all workers/replicas."""

    def __init__(
        self,
        transient: redis_state.RedisTransientState,
        *,
        limit: int,
        window_seconds: int,
    ):
        self.transient = transient
        self.limit = limit
        self.window_seconds = window_seconds

    async def allow(self, key: str) -> bool:
        return await self.transient.allow_rate_limit(
            key,
            limit=self.limit,
            window_seconds=self.window_seconds,
        )


def maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value):
        return value
    return None


def resolve_api_version(api_version: str) -> str:
    return API_VERSION_ALIASES.get(api_version, api_version)


_VERSION_MODULE_CACHE: dict[str, Any] = {}
_VERSION_MODULE_CACHE_LOCK = threading.RLock()


def load_version_module(settings: Settings, api_version: str) -> Any:
    if not API_VERSION_RE.fullmatch(api_version):
        raise HTTPException(status_code=404, detail="Unknown API version.")
    api_version = resolve_api_version(api_version)
    module_path = settings.api_dir / f"{api_version}.py"
    if not module_path.is_file():
        raise HTTPException(status_code=404, detail="Unknown API version.")
    with _VERSION_MODULE_CACHE_LOCK:
        cached_module = _VERSION_MODULE_CACHE.get(api_version)
        if cached_module is not None:
            return cached_module
        module_name = f"recroom_api_{api_version}"
        cached_module = sys.modules.get(module_name)
        if cached_module is not None:
            _VERSION_MODULE_CACHE[api_version] = cached_module
            return cached_module
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise HTTPException(status_code=500, detail="API module could not be loaded.")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            if sys.modules.get(module_name) is module:
                del sys.modules[module_name]
            raise
        _VERSION_MODULE_CACHE[api_version] = module
        return module


def _format_status_code(status_code: int) -> str:
    try:
        phrase = HTTPStatus(status_code).phrase
    except ValueError:
        phrase = "Error"
    return f"{status_code} {phrase}"


def _build_error_webhook_payload(alert_record: dict[str, Any]) -> dict[str, Any]:
    versions = ", ".join(alert_record["api_versions"])
    request_summary = json.dumps(alert_record["last_request_summary"], indent=2, sort_keys=True)
    embed = {
        "title": WEBHOOK_ALERT_TITLE,
        "color": DISCORD_RED_COLOR,
        "timestamp": alert_record["last_seen_at"],
        "fields": [
            {
                "name": "Version",
                "value": _truncate_webhook_value(versions or alert_record["latest_api_version"], 1024),
                "inline": True,
            },
            {
                "name": "Status",
                "value": _format_status_code(alert_record["latest_status_code"]),
                "inline": True,
            },
            {
                "name": "Python adapter",
                "value": alert_record["latest_adapter_file"],
                "inline": False,
            },
            {
                "name": "Endpoint",
                "value": f"{alert_record['method']} {alert_record['route_path']}",
                "inline": False,
            },
            {
                "name": "Data requested",
                "value": "```json\n" + _truncate_webhook_value(request_summary, 900) + "\n```",
                "inline": False,
            },
            {
                "name": "Error detail",
                "value": _truncate_webhook_value(alert_record["last_error_detail"] or "Unknown error.", 1024),
                "inline": False,
            },
            {
                "name": "Precise second",
                "value": alert_record["last_seen_at"],
                "inline": True,
            },
        ],
    }
    return {
        "content": WEBHOOK_ALERT_MESSAGE,
        "embeds": [embed],
        "allowed_mentions": {"parse": []},
    }


def _webhook_url_with_wait(webhook_url: str) -> str:
    parts = urllib.parse.urlsplit(webhook_url)
    query = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    query["wait"] = "true"
    return urllib.parse.urlunsplit(parts._replace(query=urllib.parse.urlencode(query)))


def _execute_discord_webhook_request(webhook_url: str, payload: dict[str, Any]) -> str | None:
    data = json.dumps(payload).encode("utf-8")
    target_url = _webhook_url_with_wait(webhook_url)
    request = urllib.request.Request(
        target_url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": "rec-room-api-restoring-server",
        },
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        response_body = response.read()
    if not response_body:
        return None
    try:
        response_json = json.loads(response_body.decode("utf-8"))
    except Exception:
        return None
    message_id_value = response_json.get("id")
    return str(message_id_value) if message_id_value else None


async def notify_endpoint_error_webhook(context: ServerContext, alert_record: dict[str, Any]) -> None:
    webhook_url = context.settings.error_webhook_url
    if not webhook_url:
        return
    if not alert_record.get("is_new"):
        return
    payload = _build_error_webhook_payload(alert_record)
    try:
        returned_message_id = await asyncio.to_thread(
            _execute_discord_webhook_request,
            webhook_url,
            payload,
        )
    except (OSError, urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
        print(f"Discord endpoint alert failed: {type(exc).__name__}", file=sys.stderr)
        return
    except Exception as exc:
        print(f"Discord endpoint alert failed unexpectedly: {type(exc).__name__}", file=sys.stderr)
        return
    if alert_record.get("is_new") and returned_message_id:
        context.set_endpoint_error_alert_message_id(alert_record["endpoint_key"], returned_message_id)


def should_alert_endpoint_status(status_code: int) -> bool:
    return status_code == 404 or status_code >= 500


def _adapter_file_label(settings: Settings, module: Any | None, resolved_api_version: str) -> str:
    module_file = getattr(module, "__file__", None)
    if module_file:
        try:
            return str(Path(module_file).resolve().relative_to(settings.root_dir))
        except ValueError:
            return Path(module_file).name
    return f"APIs/{resolved_api_version}.py (not loaded)"


def _safe_error_detail(detail: Any) -> str:
    if detail is None:
        return ""
    if isinstance(detail, str):
        return _truncate_webhook_value(detail, 1024)
    try:
        text = json.dumps(_summarize_payload_shape(detail), sort_keys=True)
    except Exception:
        text = type(detail).__name__
    return _truncate_webhook_value(text, 1024)


async def record_and_notify_endpoint_error(
    *,
    context: ServerContext,
    settings: Settings,
    request: Request,
    route_path: str,
    resolved_api_version: str,
    module: Any | None,
    status_code: int,
    error_detail: Any,
) -> None:
    request_summary = await summarize_request_data(request, route_path)
    alert_record = context.record_endpoint_error_alert(
        method=request.method,
        route_path=route_path,
        api_version=resolved_api_version,
        adapter_file=_adapter_file_label(settings, module, resolved_api_version),
        status_code=status_code,
        request_summary=request_summary,
        error_detail=_safe_error_detail(error_detail),
    )
    await notify_endpoint_error_webhook(context, alert_record)


async def fanout_maintenance_state(
    context: ServerContext,
    state: dict[str, Any],
    *,
    cancellation: bool = False,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "realtime_delivered_clients": 0,
        "snapshot_only_clients": 0,
        "unsupported_versions": [],
        "disconnected_versions": [],
        "failed_deliveries": [],
        "per_version": {},
        "process_scope": "single_process",
    }
    for api_version in MAINTENANCE_SUPPORTED_API_VERSIONS:
        try:
            load_version_module(context.settings, api_version)
        except Exception as exc:
            summary["failed_deliveries"].append(
                {"api_version": api_version, "error": type(exc).__name__}
            )
            summary["per_version"][api_version] = {
                "status": "load_error",
                "error": type(exc).__name__,
            }
    with _VERSION_MODULE_CACHE_LOCK:
        loaded_modules = list(_VERSION_MODULE_CACHE.items())
    for api_version, module in loaded_modules:
        if api_version not in MAINTENANCE_SUPPORTED_API_VERSIONS:
            summary["unsupported_versions"].append(api_version)
            summary["per_version"][api_version] = {"status": "unsupported"}
            continue
        capability_function = getattr(module, "maintenance_capabilities", None)
        if capability_function is None:
            summary["unsupported_versions"].append(api_version)
            summary["per_version"][api_version] = {"status": "unsupported"}
            continue
        try:
            capabilities = capability_function(context=context)
        except Exception as exc:
            summary["failed_deliveries"].append(
                {"api_version": api_version, "error": type(exc).__name__}
            )
            summary["per_version"][api_version] = {
                "status": "capability_error",
                "error": type(exc).__name__,
            }
            continue
        if not isinstance(capabilities, dict) or not capabilities.get("snapshot_supported"):
            summary["unsupported_versions"].append(api_version)
            summary["per_version"][api_version] = {"status": "unsupported"}
            continue
        connected_clients = max(0, int(capabilities.get("connected_clients", 0) or 0))
        if cancellation:
            if not capabilities.get("realtime_cancel_supported"):
                summary["per_version"][api_version] = {
                    "status": "realtime_cancel_unsupported",
                    "connected_clients": connected_clients,
                }
                continue
            fanout_function = getattr(module, "fanout_maintenance_cancel", None)
        else:
            if not capabilities.get("realtime_supported"):
                summary["snapshot_only_clients"] += connected_clients
                summary["per_version"][api_version] = {
                    "status": "snapshot_only",
                    "connected_clients": connected_clients,
                }
                continue
            fanout_function = getattr(module, "fanout_maintenance", None)
        if connected_clients <= 0:
            summary["disconnected_versions"].append(api_version)
            summary["per_version"][api_version] = {
                "status": "disconnected",
                "connected_clients": 0,
            }
            continue
        if fanout_function is None:
            summary["per_version"][api_version] = {
                "status": "unimplemented",
                "connected_clients": connected_clients,
            }
            continue
        try:
            delivery = fanout_function(state=state, context=context)
            awaited = maybe_await(delivery)
            if awaited is not None:
                delivery = await awaited
            if not isinstance(delivery, dict):
                raise TypeError("Maintenance fan-out must return a delivery summary.")
            delivered = max(0, int(delivery.get("delivered_clients", 0) or 0))
            failures = delivery.get("failed_deliveries", [])
            if not isinstance(failures, list):
                failures = [{"error": "Invalid failure summary."}]
            summary["realtime_delivered_clients"] += delivered
            summary["failed_deliveries"].extend(
                [{"api_version": api_version, **failure} for failure in failures if isinstance(failure, dict)]
            )
            summary["per_version"][api_version] = {
                "status": "delivered" if delivered else "failed",
                "connected_clients": connected_clients,
                "delivered_clients": delivered,
                "failed_deliveries": len(failures),
            }
        except Exception as exc:
            summary["failed_deliveries"].append(
                {"api_version": api_version, "error": type(exc).__name__}
            )
            summary["per_version"][api_version] = {
                "status": "failed",
                "connected_clients": connected_clients,
                "error": type(exc).__name__,
            }
    return summary


async def arm_maintenance_deadline(
    context: ServerContext,
    state: dict[str, Any],
) -> dict[str, Any]:
    """Arm version-local expiry behavior for the persisted maintenance state."""
    summary: dict[str, Any] = {"per_version": {}, "failed": []}
    for api_version in MAINTENANCE_SUPPORTED_API_VERSIONS:
        try:
            module = load_version_module(context.settings, api_version)
            arm = getattr(module, "arm_maintenance_deadline", None)
            if arm is None:
                summary["per_version"][api_version] = {"status": "unsupported"}
                continue
            result = arm(state=state, context=context)
            awaited = maybe_await(result)
            if awaited is not None:
                result = await awaited
            summary["per_version"][api_version] = (
                result if isinstance(result, dict) else {"status": "armed"}
            )
        except Exception as exc:
            failure = {"api_version": api_version, "error": type(exc).__name__}
            summary["failed"].append(failure)
            summary["per_version"][api_version] = {
                "status": "failed",
                "error": type(exc).__name__,
            }
    return summary


async def stop_maintenance_deadlines(context: ServerContext) -> None:
    for api_version in MAINTENANCE_SUPPORTED_API_VERSIONS:
        try:
            module = load_version_module(context.settings, api_version)
            stop = getattr(module, "stop_maintenance_deadline", None)
            if stop is None:
                continue
            result = stop(context=context)
            awaited = maybe_await(result)
            if awaited is not None:
                await awaited
        except Exception:
            continue


async def enforce_player_room_lock(
    context: ServerContext,
    *,
    player_id: str,
    sanction_type: str,
    reason: str,
    duration_seconds: int | None,
) -> dict[str, Any]:
    """Ask each dated adapter to eject a newly sanctioned connected player."""
    summary: dict[str, Any] = {"per_version": {}, "failed": []}
    for api_version in MAINTENANCE_SUPPORTED_API_VERSIONS:
        try:
            module = load_version_module(context.settings, api_version)
            enforce = getattr(module, "enforce_account_room_lock", None)
            if enforce is None:
                summary["per_version"][api_version] = {"status": "unsupported"}
                continue
            result = enforce(
                player_id=player_id,
                sanction_type=sanction_type,
                reason=reason,
                duration_seconds=duration_seconds,
                context=context,
            )
            awaited = maybe_await(result)
            if awaited is not None:
                result = await awaited
            summary["per_version"][api_version] = (
                result if isinstance(result, dict) else {"status": "enforced"}
            )
        except Exception as exc:
            failure = {"api_version": api_version, "error": type(exc).__name__}
            summary["failed"].append(failure)
            summary["per_version"][api_version] = {
                "status": "failed",
                "error": type(exc).__name__,
            }
    return summary


def maintenance_api_capabilities(context: ServerContext) -> dict[str, Any]:
    versions: dict[str, Any] = {}
    connected_clients = 0
    for api_version in MAINTENANCE_SUPPORTED_API_VERSIONS:
        try:
            module = load_version_module(context.settings, api_version)
            capability_function = getattr(module, "maintenance_capabilities", None)
            if capability_function is None:
                raise RuntimeError("Maintenance capability is not implemented.")
            capabilities = capability_function(context=context)
            if not isinstance(capabilities, dict):
                raise TypeError("Maintenance capability must be a dictionary.")
            version_connected = max(
                0,
                int(capabilities.get("connected_clients", 0) or 0),
            )
            connected_clients += version_connected
            versions[api_version] = {
                "available": True,
                "connected_clients": version_connected,
                "snapshot_supported": bool(
                    capabilities.get("snapshot_supported")
                ),
                "realtime_supported": bool(
                    capabilities.get("realtime_supported")
                ),
                "realtime_cancel_supported": bool(
                    capabilities.get("realtime_cancel_supported")
                ),
                "transport": capabilities.get("transport"),
                "hub": capabilities.get("hub"),
                "notification_id": capabilities.get("notification_id"),
            }
        except Exception as exc:
            versions[api_version] = {
                "available": False,
                "connected_clients": 0,
                "error": type(exc).__name__,
            }
    return {
        "supported_versions": list(MAINTENANCE_SUPPORTED_API_VERSIONS),
        "other_versions_supported": False,
        "connected_clients": connected_clients,
        "versions": versions,
    }


def max_maintenance_minutes() -> int:
    raw = os.getenv("RECROOM_MAX_MAINTENANCE_MINUTES")
    if raw is None:
        return DEFAULT_MAX_MAINTENANCE_MINUTES
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError("RECROOM_MAX_MAINTENANCE_MINUTES must be an integer.") from exc
    if value < 0:
        raise ConfigurationError("RECROOM_MAX_MAINTENANCE_MINUTES must be non-negative.")
    return value


def admin_actor_id(request: Request) -> str:
    # Never persist, echo, or hash the secret itself. Individual moderator
    # sessions can replace this shared emergency identity later without
    # changing the canonical audit schema.
    return str(getattr(request.state, "admin_actor_id", "emergency-admin-key"))


def moderation_confirmation_phrase(action: str, target_id: str) -> str | None:
    if action == "permanent_account_ban":
        return f"BAN {target_id}"
    if action == "remove_content":
        return f"REMOVE {target_id}"
    return None


def admin_allowed_actions(
    db: Database,
    *,
    target_type: str,
    target_id: str,
    case: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    target_type = str(target_type)
    target_id = str(target_id)
    actions: list[dict[str, Any]] = []

    def add(
        name: str,
        label: str,
        effect: str,
        *,
        temporary: bool = False,
        destructive: bool = False,
    ) -> None:
        phrase = moderation_confirmation_phrase(name, target_id)
        actions.append(
            {
                "name": name,
                "label": label,
                "effect": effect,
                "temporary": temporary,
                "requires_confirmation": destructive,
                "confirmation_phrase": phrase,
            }
        )

    case_id = str(case.get("case_id")) if case else None
    case_state = str(case.get("state")) if case else None
    if case_id and case_state not in {"observed", "cleared"}:
        add("observe", "Return to observation", "Keeps the case open without an active decision.")
    if case_id and case_state != "cleared":
        add("dismiss_case", "Dismiss case", "Closes the allegation without punishing the target.")

    if target_type == "player":
        with db.connection() as conn:
            player = conn.execute(
                "SELECT is_coach, is_banned FROM players WHERE player_id = ?",
                (target_id,),
            ).fetchone()
        if player is None or bool(player["is_coach"]):
            return actions
        sanctions = moderation_service.active_player_sanctions(db, target_id)
        account_sanction = next(
            (row for row in sanctions if str(row.get("scope")) == "account"),
            None,
        )
        invention_restriction = next(
            (
                row
                for row in sanctions
                if str(row.get("scope")) == "invention_publishing"
            ),
            None,
        )
        if account_sanction is None and not bool(player["is_banned"]):
            add(
                "timeout",
                "Apply timeout",
                "Temporarily blocks account access for the selected duration.",
                temporary=True,
            )
            add(
                "permanent_account_ban",
                "Permanently ban account",
                "Blocks account access and publishing while preserving evidence, ownership, and stored data.",
                destructive=True,
            )
        elif account_sanction is not None:
            with db.connection() as conn:
                reversible_action = conn.execute(
                    """
                    SELECT ma.action_id
                    FROM moderation_actions AS ma
                    WHERE ma.target_type = 'player'
                      AND ma.target_id = ?
                      AND ma.action IN ('timeout', 'ban')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM moderation_actions AS reversal
                          WHERE reversal.reverses_action_id = ma.action_id
                      )
                    ORDER BY ma.created_at DESC
                    LIMIT 1
                    """,
                    (target_id,),
                ).fetchone()
            if reversible_action is not None:
                actions.append(
                    {
                        "name": "reverse_action",
                        "label": "Reverse active account action",
                        "effect": "Deactivates the current account sanction and records an audited reversal.",
                        "temporary": False,
                        "requires_confirmation": False,
                        "confirmation_phrase": None,
                        "action_id": str(reversible_action["action_id"]),
                    }
                )
        if invention_restriction is None:
            add(
                "restrict_invention_publishing",
                "Restrict invention publishing",
                "Prevents creating, editing, and publishing inventions without blocking login or deleting existing inventions.",
            )
        else:
            add(
                "restore_invention_publishing",
                "Restore invention publishing",
                "Removes the active invention-publishing restriction.",
            )
    elif target_type in {"room", "invention", "player_event"} and case_id:
        active_quarantine = any(
            bool(row.get("active"))
            and str(row.get("control_type")) == "quarantine"
            for row in case.get("content_controls", [])
        )
        if active_quarantine:
            add(
                "restore_content",
                "Restore content",
                "Reverses the active quarantine and returns the item to its previous moderation state.",
            )
        else:
            add(
                "quarantine_content",
                "Quarantine content",
                "Hides this item from supported public surfaces while preserving evidence and ownership.",
            )
    return actions


def find_latest_moderation_case(
    db: Database,
    *,
    target_type: str,
    target_id: str,
) -> dict[str, Any] | None:
    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT case_id
            FROM moderation_cases
            WHERE target_type = ? AND target_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (target_type, target_id),
        ).fetchone()
    return (
        moderation_service.get_case(db, str(row["case_id"]))
        if row is not None
        else None
    )


def search_admin_targets(db: Database, query: str, *, limit: int = 50) -> list[dict[str, Any]]:
    query = str(query).strip()
    if len(query) < 2:
        raise ValueError("Search requires at least two characters.")
    limit = max(1, min(int(limit), 100))
    pattern = f"%{query}%"
    results: list[dict[str, Any]] = []
    with db.connection() as conn:
        player_rows = conn.execute(
            """
            SELECT player_id, username, display_name, canonical_level, is_banned
            FROM players
            WHERE player_id = ?
               OR username LIKE ? COLLATE NOCASE
               OR display_name LIKE ? COLLATE NOCASE
            ORDER BY
                CASE WHEN player_id = ? THEN 0
                     WHEN username = ? COLLATE NOCASE THEN 1
                     ELSE 2 END,
                username
            LIMIT ?
            """,
            (query, pattern, pattern, query, query, limit),
        ).fetchall()
        room_rows = conn.execute(
            """
            SELECT room_id, name, owner_player_id, is_official
            FROM rooms
            WHERE room_id = ? OR name LIKE ? COLLATE NOCASE
            ORDER BY CASE WHEN room_id = ? THEN 0 ELSE 1 END, name
            LIMIT ?
            """,
            (query, pattern, query, limit),
        ).fetchall()
        invention_settings = conn.execute(
            """
            SELECT key, value_json
            FROM server_settings
            WHERE key LIKE '%.inventions'
            """
        ).fetchall()
    for row in player_rows:
        results.append(
            {
                "target_type": "player",
                "target_id": str(row["player_id"]),
                "title": str(row["display_name"] or row["username"]),
                "subtitle": f"@{row['username']} · level {row['canonical_level']}",
                "status": "banned" if bool(row["is_banned"]) else "active",
            }
        )
    for row in room_rows:
        results.append(
            {
                "target_type": "room",
                "target_id": str(row["room_id"]),
                "title": str(row["name"]),
                "subtitle": "Official room" if bool(row["is_official"]) else "Player room",
                "status": "official" if bool(row["is_official"]) else "public",
            }
        )
    query_folded = query.casefold()
    for setting in invention_settings:
        try:
            records = json.loads(setting["value_json"])
        except (TypeError, ValueError):
            continue
        if not isinstance(records, list):
            continue
        for record in records:
            invention = record.get("Invention") if isinstance(record, dict) else None
            if not isinstance(invention, dict):
                continue
            invention_id = str(invention.get("InventionId") or "")
            name = str(invention.get("Name") or "")
            if query != invention_id and query_folded not in name.casefold():
                continue
            results.append(
                {
                    "target_type": "invention",
                    "target_id": invention_id,
                    "title": name or f"Invention {invention_id}",
                    "subtitle": f"Creator legacy ID {invention.get('CreatorPlayerId') or 'unknown'}",
                    "status": "published" if bool(invention.get("IsPublished")) else "private",
                }
            )
            if len(results) >= limit:
                break
        if len(results) >= limit:
            break
    return results[:limit]


def admin_player_detail(db: Database, player_id: str) -> dict[str, Any] | None:
    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT player_id, username, display_name, email, verified,
                   permissions_json, canonical_level, canonical_xp,
                   profile_picture_asset_id, is_coach, is_banned,
                   banned_at, ban_reason, created_at, updated_at
            FROM players
            WHERE player_id = ?
            """,
            (player_id,),
        ).fetchone()
        if row is None:
            return None
        rooms = conn.execute(
            """
            SELECT room_id, name, is_official, updated_at
            FROM rooms
            WHERE owner_player_id = ? OR creator_player_id = ?
            ORDER BY updated_at DESC
            LIMIT 100
            """,
            (player_id, player_id),
        ).fetchall()
        cases = conn.execute(
            """
            SELECT case_id, canonical_category, state, report_count, updated_at
            FROM moderation_cases
            WHERE target_type = 'player' AND target_id = ?
            ORDER BY updated_at DESC
            LIMIT 100
            """,
            (player_id,),
        ).fetchall()
    player = dict(row)
    try:
        player["permissions"] = json.loads(player.pop("permissions_json"))
    except (TypeError, ValueError):
        player["permissions"] = []
    player["verified"] = bool(player["verified"])
    player["is_coach"] = bool(player["is_coach"])
    player["is_banned"] = bool(player["is_banned"])
    player["active_sanctions"] = moderation_service.active_player_sanctions(
        db,
        player_id,
    )
    player["rooms"] = [dict(item) for item in rooms]
    player["cases"] = [dict(item) for item in cases]
    latest_case = find_latest_moderation_case(
        db,
        target_type="player",
        target_id=player_id,
    )
    player["allowed_actions"] = admin_allowed_actions(
        db,
        target_type="player",
        target_id=player_id,
        case=latest_case,
    )
    player["latest_case_id"] = latest_case.get("case_id") if latest_case else None
    return player


def admin_player_summary(db: Database, player_id: str) -> dict[str, Any] | None:
    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT player_id, username, display_name, canonical_level
            FROM players
            WHERE player_id = ?
            """,
            (str(player_id),),
        ).fetchone()
    if row is None:
        return None
    summary = dict(row)
    summary["display_name"] = str(
        summary.get("display_name") or summary.get("username") or "Player"
    )
    return summary


def enrich_admin_moderation_case(
    db: Database,
    case: dict[str, Any],
) -> dict[str, Any]:
    if (
        str(case.get("target_type") or "").casefold() == "player"
        and case.get("target_id")
    ):
        case["target_player"] = admin_player_summary(
            db,
            str(case["target_id"]),
        )
    reports = case.get("reports")
    if isinstance(reports, list):
        for report in reports:
            if not isinstance(report, dict):
                continue
            reporter_id = str(report.get("reporter_player_id") or "")
            report["reporter_player"] = (
                admin_player_summary(db, reporter_id)
                if reporter_id
                else None
            )
            # The case drawer is already an administrator-only surface. Report
            # wording must remain readable there even for older rows whose
            # public copy was censored before report text was exempted.
            report_id = str(report.get("report_id") or "")
            if report_id:
                with db.connection() as conn:
                    evidence = conn.execute(
                        """
                        SELECT raw_text
                        FROM moderation_evidence
                        WHERE report_id = ?
                          AND evidence_type = 'report_details'
                          AND deleted_at IS NULL
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (report_id,),
                    ).fetchone()
                if evidence is not None and evidence["raw_text"] is not None:
                    report["public_details"] = str(evidence["raw_text"])
    return case


def admin_content_detail(
    db: Database,
    *,
    target_type: str,
    target_id: str,
) -> dict[str, Any] | None:
    target_type = str(target_type)
    target_id = str(target_id)
    content: dict[str, Any] | None = None
    if target_type == "room":
        with db.connection() as conn:
            row = conn.execute(
                """
                SELECT room_id, owner_player_id, creator_player_id, name,
                       is_official, is_coach_only_edit, created_by_system,
                       metadata_json, created_at, updated_at
                FROM rooms
                WHERE room_id = ?
                """,
                (target_id,),
            ).fetchone()
        if row is not None:
            content = dict(row)
            for key in ("is_official", "is_coach_only_edit", "created_by_system"):
                content[key] = bool(content[key])
            try:
                content["metadata"] = json.loads(content.pop("metadata_json"))
            except (TypeError, ValueError):
                content["metadata"] = {}
    elif target_type == "invention":
        with db.connection() as conn:
            settings_rows = conn.execute(
                "SELECT key, value_json FROM server_settings WHERE key LIKE '%.inventions'"
            ).fetchall()
        for setting in settings_rows:
            try:
                records = json.loads(setting["value_json"])
            except (TypeError, ValueError):
                continue
            if not isinstance(records, list):
                continue
            for record in records:
                invention = record.get("Invention") if isinstance(record, dict) else None
                if isinstance(invention, dict) and str(
                    invention.get("InventionId") or ""
                ) == target_id:
                    content = {
                        "source_key": str(setting["key"]),
                        "invention": invention,
                        "auto_tags": list(record.get("AutoTags") or []),
                        "player_added_tags": list(record.get("PlayerAddedTags") or []),
                    }
                    break
            if content is not None:
                break
    latest_case = find_latest_moderation_case(
        db,
        target_type=target_type,
        target_id=target_id,
    )
    if content is None and latest_case is None:
        return None
    result = content or {"target_type": target_type, "target_id": target_id}
    result["target_type"] = target_type
    result["target_id"] = target_id
    result["latest_case_id"] = latest_case.get("case_id") if latest_case else None
    result["allowed_actions"] = admin_allowed_actions(
        db,
        target_type=target_type,
        target_id=target_id,
        case=latest_case,
    )
    return result


def insert_bug_report(
    conn: sqlite3.Connection,
    *,
    report_id: str,
    reporter_player_id: str | None,
    reporter_legacy_id: int,
    summary: str,
    description: str,
    test_case_key: str,
    build_version: str,
    build_timestamp: int,
    bundle_version_code: int | None,
    screenshot_blob_name: str | None,
    output_log_blob_name: str | None,
    source_version: str,
    source_endpoint: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Insert one exact-client bug report and its immutable submission audit."""
    timestamp = created_at or utc_now()
    conn.execute(
        """
        INSERT INTO bug_reports(
            report_id, reporter_player_id, reporter_legacy_id, summary,
            description, test_case_key, build_version, build_timestamp,
            bundle_version_code, screenshot_blob_name, output_log_blob_name,
            group_id, status, dismissed_by, dismissed_at, dismiss_reason,
            source_version, source_endpoint, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'open',
                  NULL, NULL, NULL, ?, ?, ?, ?)
        """,
        (
            report_id,
            reporter_player_id,
            max(0, int(reporter_legacy_id)),
            str(summary)[:500],
            str(description)[:20_000],
            str(test_case_key)[:500],
            str(build_version)[:200],
            int(build_timestamp),
            bundle_version_code,
            screenshot_blob_name or None,
            output_log_blob_name or None,
            str(source_version)[:100],
            str(source_endpoint)[:300],
            timestamp,
            timestamp,
        ),
    )
    conn.execute(
        """
        INSERT INTO bug_report_actions(
            action_id, action, actor_id, target_type, target_id,
            report_id, group_id, reason, previous_state, new_state,
            idempotency_key, metadata_json, created_at
        ) VALUES (?, 'submitted', ?, 'bug_report', ?, ?, NULL, NULL,
                  NULL, 'open', NULL, ?, ?)
        """,
        (
            uuid.uuid4().hex,
            f"player:{max(0, int(reporter_legacy_id))}",
            report_id,
            report_id,
            json.dumps(
                {
                    "source_version": str(source_version),
                    "source_endpoint": str(source_endpoint),
                    "has_screenshot": bool(screenshot_blob_name),
                    "has_output_log": bool(output_log_blob_name),
                },
                sort_keys=True,
            ),
            timestamp,
        ),
    )
    return {
        "report_id": report_id,
        "status": "open",
        "created_at": timestamp,
    }


def migrate_legacy_bug_reports(db: Database) -> None:
    """Import the adapter's former JSON queue without deleting its source."""
    with db.transaction() as conn:
        settings = conn.execute(
            """
            SELECT key, value_json
            FROM server_settings
            WHERE key LIKE '%.bug_reports'
            """
        ).fetchall()
        for setting in settings:
            source_version = str(setting["key"]).removesuffix(".bug_reports")
            try:
                reports = json.loads(setting["value_json"] or "[]")
            except (TypeError, ValueError):
                continue
            if not isinstance(reports, list):
                continue
            for raw in reports:
                if not isinstance(raw, dict):
                    continue
                report_id = str(raw.get("ReportId") or "").strip()
                if not report_id:
                    continue
                exists = conn.execute(
                    "SELECT 1 FROM bug_reports WHERE report_id = ?",
                    (report_id,),
                ).fetchone()
                if exists is not None:
                    continue
                try:
                    reporter_legacy_id = max(0, int(raw.get("PlayerId") or 0))
                except (TypeError, ValueError):
                    reporter_legacy_id = 0
                reporter = conn.execute(
                    """
                    SELECT p.player_id
                    FROM players AS p
                    JOIN player_version_state AS pvs
                      ON pvs.player_id = p.player_id
                    WHERE pvs.api_version = ?
                      AND (
                        CAST(json_extract(pvs.state_json, '$.legacy_player_id') AS INTEGER) = ?
                        OR CAST(json_extract(pvs.state_json, '$.recnet_id') AS INTEGER) = ?
                      )
                    LIMIT 1
                    """,
                    (source_version, reporter_legacy_id, reporter_legacy_id),
                ).fetchone()
                bundle_value = raw.get("BundleVersionCode")
                try:
                    bundle_version_code = (
                        int(bundle_value) if bundle_value is not None else None
                    )
                except (TypeError, ValueError):
                    bundle_version_code = None
                try:
                    build_timestamp = int(raw.get("BuildTimestamp") or 0)
                except (TypeError, ValueError):
                    build_timestamp = 0
                insert_bug_report(
                    conn,
                    report_id=report_id,
                    reporter_player_id=(
                        str(reporter["player_id"]) if reporter is not None else None
                    ),
                    reporter_legacy_id=reporter_legacy_id,
                    summary=str(raw.get("Summary") or ""),
                    description=str(raw.get("Description") or ""),
                    test_case_key=str(raw.get("TestCaseKey") or ""),
                    build_version=str(raw.get("BuildVersion") or ""),
                    build_timestamp=build_timestamp,
                    bundle_version_code=bundle_version_code,
                    screenshot_blob_name=str(
                        raw.get("ScreenshotBlobName") or ""
                    ) or None,
                    output_log_blob_name=str(
                        raw.get("OutputLogBlobName") or ""
                    ) or None,
                    source_version=source_version,
                    source_endpoint="legacy-json-import",
                    created_at=str(raw.get("CreatedAt") or utc_now()),
                )


def _bug_report_from_row(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["has_screenshot"] = bool(item.pop("screenshot_blob_name", None))
    item["has_output_log"] = bool(item.pop("output_log_blob_name", None))
    reporter_player_id = item.pop("reporter_player_id", None)
    username = item.pop("reporter_username", None)
    display_name = item.pop("reporter_display_name", None)
    item["reporter"] = {
        "player_id": reporter_player_id,
        "legacy_player_id": int(item.get("reporter_legacy_id") or 0),
        "username": username,
        "display_name": display_name,
    }
    return item


def list_bug_reports(
    db: Database,
    *,
    status_filter: str | None,
    query: str | None,
    group_id: str | None,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))
    clauses: list[str] = []
    params: list[Any] = []
    if status_filter:
        clauses.append("br.status = ?")
        params.append(status_filter)
    if group_id:
        if group_id == "ungrouped":
            clauses.append("br.group_id IS NULL")
        else:
            clauses.append("br.group_id = ?")
            params.append(group_id)
    if query:
        pattern = f"%{query.strip()}%"
        clauses.append(
            """
            (
                br.report_id LIKE ? COLLATE NOCASE
                OR br.summary LIKE ? COLLATE NOCASE
                OR br.description LIKE ? COLLATE NOCASE
                OR p.username LIKE ? COLLATE NOCASE
                OR p.display_name LIKE ? COLLATE NOCASE
            )
            """
        )
        params.extend([pattern] * 5)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    with db.connection() as conn:
        total = int(
            conn.execute(
                f"""
                SELECT COUNT(*) AS total
                FROM bug_reports AS br
                LEFT JOIN players AS p ON p.player_id = br.reporter_player_id
                {where}
                """,
                params,
            ).fetchone()["total"]
        )
        rows = conn.execute(
            f"""
            SELECT br.*, p.username AS reporter_username,
                   p.display_name AS reporter_display_name,
                   bg.title AS group_title,
                   (
                       SELECT COUNT(*)
                       FROM bug_reports AS grouped
                       WHERE grouped.group_id = br.group_id
                         AND br.group_id IS NOT NULL
                   ) AS group_report_count
            FROM bug_reports AS br
            LEFT JOIN players AS p ON p.player_id = br.reporter_player_id
            LEFT JOIN bug_report_groups AS bg ON bg.group_id = br.group_id
            {where}
            ORDER BY CASE br.status WHEN 'open' THEN 0 ELSE 1 END,
                     br.created_at DESC
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ).fetchall()
    return [_bug_report_from_row(row) for row in rows], total


def list_bug_report_groups(db: Database) -> list[dict[str, Any]]:
    with db.connection() as conn:
        rows = conn.execute(
            """
            SELECT bg.group_id, bg.title, bg.state, bg.created_by,
                   bg.created_at, bg.updated_at,
                   COUNT(br.report_id) AS report_count,
                   SUM(CASE WHEN br.status = 'open' THEN 1 ELSE 0 END) AS open_count
            FROM bug_report_groups AS bg
            LEFT JOIN bug_reports AS br ON br.group_id = bg.group_id
            GROUP BY bg.group_id
            ORDER BY bg.updated_at DESC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def get_bug_report(db: Database, report_id: str) -> dict[str, Any] | None:
    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT br.*, p.username AS reporter_username,
                   p.display_name AS reporter_display_name,
                   bg.title AS group_title,
                   (
                       SELECT COUNT(*)
                       FROM bug_reports AS grouped
                       WHERE grouped.group_id = br.group_id
                         AND br.group_id IS NOT NULL
                   ) AS group_report_count
            FROM bug_reports AS br
            LEFT JOIN players AS p ON p.player_id = br.reporter_player_id
            LEFT JOIN bug_report_groups AS bg ON bg.group_id = br.group_id
            WHERE br.report_id = ?
            """,
            (report_id,),
        ).fetchone()
        if row is None:
            return None
        actions = conn.execute(
            """
            SELECT action_id, action, actor_id, target_type, target_id,
                   report_id, group_id, reason, previous_state, new_state,
                   metadata_json, created_at
            FROM bug_report_actions
            WHERE report_id = ? OR (
                group_id IS NOT NULL
                AND group_id = (SELECT group_id FROM bug_reports WHERE report_id = ?)
            )
            ORDER BY created_at
            """,
            (report_id, report_id),
        ).fetchall()
        group_members = conn.execute(
            """
            SELECT report_id, summary, status, reporter_legacy_id, created_at
            FROM bug_reports
            WHERE group_id IS NOT NULL
              AND group_id = (SELECT group_id FROM bug_reports WHERE report_id = ?)
            ORDER BY created_at
            """,
            (report_id,),
        ).fetchall()
    result = _bug_report_from_row(row)
    result["actions"] = []
    for action in actions:
        entry = dict(action)
        try:
            entry["metadata"] = json.loads(entry.pop("metadata_json") or "{}")
        except (TypeError, ValueError):
            entry["metadata"] = {}
        result["actions"].append(entry)
    result["group_members"] = [dict(member) for member in group_members]
    return result


def get_bug_report_attachment(
    db: Database,
    report_id: str,
    kind: str,
) -> tuple[str, bytes] | None:
    column = {
        "screenshot": "screenshot_blob_name",
        "log": "output_log_blob_name",
    }.get(kind)
    if column is None:
        return None
    with db.connection() as conn:
        report = conn.execute(
            f"""
            SELECT {column} AS blob_name, source_version
            FROM bug_reports
            WHERE report_id = ?
            """,
            (report_id,),
        ).fetchone()
        if report is None or not report["blob_name"]:
            return None
        blob = conn.execute(
            """
            SELECT blob_name, data
            FROM room_data_blobs
            WHERE blob_name = ?
              AND room_id = ?
            """,
            (
                str(report["blob_name"]),
                f"bugreport:{report['source_version']}:{report_id}",
            ),
        ).fetchone()
    if blob is None:
        return None
    return str(blob["blob_name"]), bytes(blob["data"])


def dismiss_bug_report(
    db: Database,
    *,
    report_id: str,
    actor_id: str,
    reason: str,
    idempotency_key: str,
) -> tuple[dict[str, Any], bool, bool]:
    now = utc_now()
    with db.transaction() as conn:
        replay = conn.execute(
            """
            SELECT 1
            FROM bug_report_actions
            WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()
        report = conn.execute(
            "SELECT status FROM bug_reports WHERE report_id = ?",
            (report_id,),
        ).fetchone()
        if report is None:
            raise KeyError(report_id)
        if replay is not None:
            changed = False
            idempotent_replay = True
        elif str(report["status"]) == "dismissed":
            changed = False
            idempotent_replay = False
        else:
            conn.execute(
                """
                UPDATE bug_reports
                SET status = 'dismissed', dismissed_by = ?,
                    dismissed_at = ?, dismiss_reason = ?, updated_at = ?
                WHERE report_id = ?
                """,
                (actor_id, now, reason, now, report_id),
            )
            conn.execute(
                """
                INSERT INTO bug_report_actions(
                    action_id, action, actor_id, target_type, target_id,
                    report_id, group_id, reason, previous_state, new_state,
                    idempotency_key, metadata_json, created_at
                ) VALUES (?, 'dismissed', ?, 'bug_report', ?, ?,
                          (SELECT group_id FROM bug_reports WHERE report_id = ?),
                          ?, ?, 'dismissed', ?, '{}', ?)
                """,
                (
                    uuid.uuid4().hex,
                    actor_id,
                    report_id,
                    report_id,
                    report_id,
                    reason,
                    str(report["status"]),
                    idempotency_key,
                    now,
                ),
            )
            changed = True
            idempotent_replay = False
    updated = get_bug_report(db, report_id)
    assert updated is not None
    return updated, changed, idempotent_replay


def group_bug_reports(
    db: Database,
    *,
    report_ids: list[str],
    group_id: str | None,
    title: str | None,
    actor_id: str,
    reason: str,
    idempotency_key: str,
) -> tuple[dict[str, Any], bool, bool]:
    unique_ids = list(dict.fromkeys(str(value).strip() for value in report_ids))
    if not unique_ids or any(not value for value in unique_ids):
        raise ValueError("At least one valid report ID is required.")
    if len(unique_ids) > 100:
        raise ValueError("No more than 100 reports can be grouped at once.")
    now = utc_now()
    with db.transaction() as conn:
        replay = conn.execute(
            """
            SELECT group_id
            FROM bug_report_actions
            WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()
        if replay is not None:
            replay_group_id = str(replay["group_id"] or "")
            group = conn.execute(
                "SELECT * FROM bug_report_groups WHERE group_id = ?",
                (replay_group_id,),
            ).fetchone()
            if group is None:
                raise ValueError("The replayed bug-report group no longer exists.")
            result = dict(group)
            result["report_ids"] = unique_ids
            return result, False, True

        placeholders = ",".join("?" for _ in unique_ids)
        reports = conn.execute(
            f"""
            SELECT report_id, summary, group_id
            FROM bug_reports
            WHERE report_id IN ({placeholders})
            """,
            unique_ids,
        ).fetchall()
        if len(reports) != len(unique_ids):
            found = {str(row["report_id"]) for row in reports}
            missing = next(value for value in unique_ids if value not in found)
            raise KeyError(missing)

        resolved_group_id = str(group_id or "").strip()
        if resolved_group_id:
            group = conn.execute(
                "SELECT * FROM bug_report_groups WHERE group_id = ?",
                (resolved_group_id,),
            ).fetchone()
            if group is None:
                raise KeyError(resolved_group_id)
        else:
            if len(unique_ids) < 2:
                raise ValueError(
                    "Select at least two reports when creating a recurring-bug group."
                )
            resolved_group_id = uuid.uuid4().hex
            resolved_title = str(title or "").strip()[:500]
            if not resolved_title:
                resolved_title = str(reports[0]["summary"] or "Recurring bug")[:500]
            conn.execute(
                """
                INSERT INTO bug_report_groups(
                    group_id, title, state, created_by, created_at, updated_at
                ) VALUES (?, ?, 'open', ?, ?, ?)
                """,
                (resolved_group_id, resolved_title, actor_id, now, now),
            )
            group = conn.execute(
                "SELECT * FROM bug_report_groups WHERE group_id = ?",
                (resolved_group_id,),
            ).fetchone()

        previous_groups = {
            str(row["report_id"]): row["group_id"] for row in reports
        }
        changed = any(
            str(row["group_id"] or "") != resolved_group_id for row in reports
        )
        if changed:
            conn.execute(
                f"""
                UPDATE bug_reports
                SET group_id = ?, updated_at = ?
                WHERE report_id IN ({placeholders})
                """,
                [resolved_group_id, now, *unique_ids],
            )
            conn.execute(
                """
                UPDATE bug_report_groups
                SET updated_at = ?
                WHERE group_id = ?
                """,
                (now, resolved_group_id),
            )
            conn.execute(
                """
                INSERT INTO bug_report_actions(
                    action_id, action, actor_id, target_type, target_id,
                    report_id, group_id, reason, previous_state, new_state,
                    idempotency_key, metadata_json, created_at
                ) VALUES (?, 'grouped', ?, 'bug_report_group', ?, NULL, ?,
                          ?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid.uuid4().hex,
                    actor_id,
                    resolved_group_id,
                    resolved_group_id,
                    reason,
                    json.dumps(previous_groups, sort_keys=True),
                    resolved_group_id,
                    idempotency_key,
                    json.dumps({"report_ids": unique_ids}, sort_keys=True),
                    now,
                ),
            )
        result = dict(group)
        result["report_ids"] = unique_ids
    return result, changed, False


def ungroup_bug_report(
    db: Database,
    *,
    report_id: str,
    actor_id: str,
    reason: str,
    idempotency_key: str,
) -> tuple[dict[str, Any], bool, bool]:
    now = utc_now()
    with db.transaction() as conn:
        replay = conn.execute(
            "SELECT 1 FROM bug_report_actions WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        report = conn.execute(
            "SELECT group_id FROM bug_reports WHERE report_id = ?",
            (report_id,),
        ).fetchone()
        if report is None:
            raise KeyError(report_id)
        previous_group = str(report["group_id"] or "")
        if replay is not None:
            changed = False
            idempotent_replay = True
        elif not previous_group:
            changed = False
            idempotent_replay = False
        else:
            conn.execute(
                """
                UPDATE bug_reports
                SET group_id = NULL, updated_at = ?
                WHERE report_id = ?
                """,
                (now, report_id),
            )
            conn.execute(
                """
                UPDATE bug_report_groups
                SET updated_at = ?
                WHERE group_id = ?
                """,
                (now, previous_group),
            )
            conn.execute(
                """
                INSERT INTO bug_report_actions(
                    action_id, action, actor_id, target_type, target_id,
                    report_id, group_id, reason, previous_state, new_state,
                    idempotency_key, metadata_json, created_at
                ) VALUES (?, 'ungrouped', ?, 'bug_report', ?, ?, ?, ?,
                          ?, NULL, ?, '{}', ?)
                """,
                (
                    uuid.uuid4().hex,
                    actor_id,
                    report_id,
                    report_id,
                    previous_group,
                    reason,
                    previous_group,
                    idempotency_key,
                    now,
                ),
            )
            changed = True
            idempotent_replay = False
    updated = get_bug_report(db, report_id)
    assert updated is not None
    return updated, changed, idempotent_replay


def list_admin_audit(
    db: Database,
    *,
    action: str | None,
    target: str | None,
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))
    clauses: list[str] = []
    params: list[Any] = []
    if action:
        clauses.append("action = ?")
        params.append(action)
    if target:
        clauses.append("(target_id = ? OR case_id = ?)")
        params.extend([target, target])
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    params.extend([limit, offset])
    with db.connection() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM (
                SELECT action_id AS audit_id, 'moderation' AS category, actor_id,
                       action, target_type, target_id, case_id, reason,
                       previous_state, new_state, metadata_json, created_at
                FROM moderation_actions
                UNION ALL
                SELECT audit_id, 'maintenance' AS category, actor_id,
                       action, 'server' AS target_type,
                       'maintenance' AS target_id, NULL AS case_id, reason,
                       previous_state_json AS previous_state,
                       new_state_json AS new_state, '{{}}' AS metadata_json,
                       created_at
                FROM maintenance_audit
                UNION ALL
                SELECT action_id AS audit_id, 'bug_report' AS category, actor_id,
                       action, target_type, target_id, group_id AS case_id, reason,
                       previous_state, new_state, metadata_json, created_at
                FROM bug_report_actions
            ) AS audit_entries
            {where}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            params,
        ).fetchall()
    results = [dict(row) for row in rows]
    for row in results:
        try:
            row["metadata"] = json.loads(row.pop("metadata_json"))
        except (TypeError, ValueError):
            row["metadata"] = {}
    return results


def create_app() -> FastAPI:
    settings = load_settings()
    try:
        transient = redis_state.build_transient_state(production=settings.is_railway)
    except redis_state.RedisConfigurationError as exc:
        raise ConfigurationError(str(exc)) from exc
    legacy_image_moves = ensure_runtime_directories(settings)
    db = Database(settings.db_path)
    initialize_database(db)
    migrate_legacy_data_asset_records(db, legacy_image_moves)
    migrate_legacy_bug_reports(db)
    refresh_railway_content_filter_snapshot(settings)
    content_filter = ContentFilter(
        settings.root_dir / FILTER_SNAPSHOT_DIR_NAME,
        enabled=environment_enabled(FILTERS),
        replacement=os.getenv("RECROOM_FILTER_REPLACEMENT", FILTER_REPLACEMENT),
        allowed_words=environment_allowed_words(FILTER_ALLOWED_WORDS),
    )
    print(content_filter.startup_summary(), file=sys.stderr)
    timed_content.reconcile_due_timed_content(db, now_utc=utc_datetime_now())
    context = ServerContext(settings, db, content_filter, transient)
    limiter = RateLimiter(
        transient,
        limit=120 if settings.is_railway else 600,
        window_seconds=60,
    )
    admin_limiter = RateLimiter(transient, limit=120, window_seconds=60)
    admin_panel_dir = settings.root_dir / ADMIN_PANEL_DIR_NAME

    app = FastAPI(
        title="Rec Room API Restoring Server",
        debug=False,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )
    admin_bearer = HTTPBearer(
        auto_error=False,
        scheme_name="AdminBearer",
        description=(
            "Emergency server administrator secret. Send it as "
            "`Authorization: Bearer <secret>`."
        ),
    )

    def request_uses_https(request: Request) -> bool:
        forwarded_proto = _first_header_value(request.headers.get("x-forwarded-proto"))
        return (forwarded_proto or request.url.scheme).casefold() == "https"

    def expected_request_origin(request: Request) -> str:
        proto = _first_header_value(request.headers.get("x-forwarded-proto")) or request.url.scheme
        host = (
            _first_header_value(request.headers.get("x-forwarded-host"))
            or _first_header_value(request.headers.get("host"))
            or request.url.netloc
        )
        return f"{proto}://{host}".rstrip("/")

    def require_same_origin(request: Request) -> None:
        origin = str(request.headers.get("origin") or "").strip().rstrip("/")
        if not origin or not hmac.compare_digest(origin, expected_request_origin(request)):
            raise HTTPException(status_code=403, detail="Invalid admin request origin.")

    async def require_operator_request(
        request: Request,
        *,
        write: bool | None = None,
    ) -> str:
        if write is None:
            write = request.method.upper() not in {"GET", "HEAD", "OPTIONS"}
        expected = configured_admin_key()
        minimum_length = 124 if settings.is_railway else 64
        if not expected or len(expected) < minimum_length:
            raise HTTPException(status_code=503, detail="Admin API key is not configured.")

        session_token = str(request.cookies.get(ADMIN_SESSION_COOKIE_NAME) or "")
        if session_token:
            session = await get_admin_session(
                transient,
                session_token,
                configured_secret=expected,
                touch=True,
            )
            if session is not None:
                if write:
                    require_same_origin(request)
                    provided_csrf = str(
                        request.headers.get(ADMIN_CSRF_HEADER) or ""
                    )
                    if not provided_csrf or not hmac.compare_digest(
                        provided_csrf,
                        str(session["csrf_token"]),
                    ):
                        raise HTTPException(
                            status_code=403,
                            detail="Valid CSRF token is required.",
                        )
                actor_id = str(session["operator_id"])
                request.state.admin_actor_id = actor_id
                request.state.admin_session_id = str(session["session_id"])
                request.state.admin_session = session
                return actor_id

        require_admin_key(request)
        request.state.admin_actor_id = "emergency-admin-key"
        request.state.admin_session_id = None
        return "emergency-admin-key"

    async def require_documented_admin(
        request: Request,
        _: HTTPAuthorizationCredentials | None = Security(admin_bearer),
    ) -> None:
        await require_operator_request(request)

    @app.exception_handler(ConfigurationError)
    async def configuration_error_handler(_: Request, exc: ConfigurationError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": str(exc)})

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_: Request, __: Exception) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": "Internal server error."})

    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next: Callable[[Request], Any]) -> Response:
        raw_content_length = request.headers.get("content-length")
        if settings.max_request_body_bytes and raw_content_length:
            try:
                content_length = int(raw_content_length)
            except ValueError:
                return JSONResponse(status_code=400, content={"detail": "Invalid Content-Length header."})
            if content_length > settings.max_request_body_bytes:
                return JSONResponse(status_code=413, content={"detail": "Request body is too large."})

        client_host = request.client.host if request.client else "unknown"
        try:
            if not await limiter.allow(f"http:{client_host}"):
                return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded."})
            if request.url.path.casefold().startswith("/admin/") and not await admin_limiter.allow(
                f"admin:{client_host}"
            ):
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Administrator rate limit exceeded."},
                )
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
        response = await call_next(request)
        if request.url.path.casefold() == "/admin" or request.url.path.casefold().startswith(
            "/admin/"
        ):
            response.headers["Content-Security-Policy"] = (
                "default-src 'none'; script-src 'self'; style-src 'self'; "
                "img-src 'self' data:; connect-src 'self'; form-action 'self'; "
                "base-uri 'none'; frame-ancestors 'none'"
            )
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["Referrer-Policy"] = "no-referrer"
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["Cache-Control"] = "no-store"
            if settings.is_railway or request_uses_https(request):
                response.headers["Strict-Transport-Security"] = (
                    "max-age=31536000; includeSubDomains"
                )
        return response

    @app.get("/health", include_in_schema=False)
    async def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/admin", include_in_schema=False)
    async def admin_panel_shell() -> Response:
        panel_path = admin_panel_dir / "index.html"
        if not panel_path.is_file():
            raise HTTPException(status_code=503, detail="Admin panel is not installed.")
        return HTMLResponse(panel_path.read_text(encoding="utf-8"))

    @app.get("/admin/assets/app.js", include_in_schema=False)
    async def admin_panel_javascript() -> Response:
        asset_path = admin_panel_dir / "app.js"
        if not asset_path.is_file():
            raise HTTPException(status_code=404, detail="Admin panel asset not found.")
        return FileResponse(
            asset_path,
            media_type="application/javascript; charset=utf-8",
            filename=None,
        )

    @app.get("/admin/assets/styles.css", include_in_schema=False)
    async def admin_panel_stylesheet() -> Response:
        asset_path = admin_panel_dir / "styles.css"
        if not asset_path.is_file():
            raise HTTPException(status_code=404, detail="Admin panel asset not found.")
        return FileResponse(
            asset_path,
            media_type="text/css; charset=utf-8",
            filename=None,
        )

    @app.post("/admin/auth/login")
    async def admin_login(request: Request) -> JSONResponse:
        expected = configured_admin_key()
        minimum_length = 124 if settings.is_railway else 64
        if not expected or len(expected) < minimum_length:
            raise HTTPException(status_code=503, detail="Admin API key is not configured.")
        origin = str(request.headers.get("origin") or "").strip()
        if origin:
            require_same_origin(request)
        client_host = request.client.host if request.client else "unknown"
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail="Admin login payload must be JSON.",
            ) from exc
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400,
                detail="Admin login payload must be a JSON object.",
            )
        supplied_value = payload.get("key")
        supplied = supplied_value if isinstance(supplied_value, str) else ""
        if not supplied or len(supplied) > 4096:
            supplied = ""
            raise HTTPException(status_code=403, detail="Invalid administrator key.")
        valid = hmac.compare_digest(supplied, expected)
        supplied = ""
        if not valid:
            if not await transient.allow_rate_limit(
                f"admin-login-failure:{client_host}", limit=10, window_seconds=15 * 60
            ):
                raise HTTPException(status_code=429, detail="Too many failed login attempts.")
            await asyncio.sleep(0.1)
            raise HTTPException(status_code=403, detail="Invalid administrator key.")
        token, session = await create_admin_session(
            transient,
            operator_id="primary_operator",
            secret=expected,
            ttl_seconds=admin_session_ttl_seconds(),
        )
        response = JSONResponse(
            {
                "Success": True,
                "Operator": session["operator_id"],
                "ExpiresAt": session["expires_at"],
                "CsrfToken": session["csrf_token"],
            }
        )
        response.set_cookie(
            ADMIN_SESSION_COOKIE_NAME,
            token,
            max_age=admin_session_ttl_seconds(),
            expires=admin_session_ttl_seconds(),
            path="/admin",
            secure=settings.is_railway or request_uses_https(request),
            httponly=True,
            samesite="strict",
        )
        return response

    @app.get("/admin/auth/session")
    async def admin_session_status(request: Request) -> JSONResponse:
        await require_operator_request(request, write=False)
        session = getattr(request.state, "admin_session", None)
        if session is None:
            return JSONResponse(
                {
                    "Success": True,
                    "Operator": "emergency-admin-key",
                    "SessionType": "emergency_api",
                    "CsrfToken": None,
                }
            )
        return JSONResponse(
            {
                "Success": True,
                "Operator": str(session["operator_id"]),
                "SessionType": "browser",
                "CreatedAt": str(session["created_at"]),
                "ExpiresAt": str(session["expires_at"]),
                "CsrfToken": str(session["csrf_token"]),
            }
        )

    @app.post("/admin/auth/logout")
    async def admin_logout(request: Request) -> JSONResponse:
        await require_operator_request(request, write=True)
        token = str(request.cookies.get(ADMIN_SESSION_COOKIE_NAME) or "")
        await revoke_admin_session(transient, token)
        response = JSONResponse({"Success": True})
        response.delete_cookie(
            ADMIN_SESSION_COOKIE_NAME,
            path="/admin",
            httponly=True,
            secure=settings.is_railway or request_uses_https(request),
            samesite="strict",
        )
        return response

    @app.post("/admin/maintenance")
    async def admin_schedule_maintenance(request: Request) -> JSONResponse:
        await require_operator_request(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Maintenance payload must be JSON.") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Maintenance payload must be a JSON object.")
        starts_in_minutes = payload.get("starts_in_minutes")
        if isinstance(starts_in_minutes, bool) or not isinstance(starts_in_minutes, int):
            raise HTTPException(
                status_code=422,
                detail="starts_in_minutes must be a non-negative JSON integer.",
            )
        if starts_in_minutes < 0:
            raise HTTPException(status_code=422, detail="starts_in_minutes must be non-negative.")
        maximum = max_maintenance_minutes()
        if starts_in_minutes > maximum:
            raise HTTPException(
                status_code=422,
                detail=f"starts_in_minutes must not exceed {maximum}.",
            )
        reason_value = payload.get("reason")
        reason = str(reason_value).strip()[:2000] if reason_value is not None else None
        idempotency_value = payload.get("idempotency_key")
        idempotency_key = (
            str(idempotency_value).strip()[:200]
            if idempotency_value is not None
            else None
        )
        if idempotency_value is not None and not idempotency_key:
            raise HTTPException(status_code=422, detail="idempotency_key cannot be empty.")
        state, changed = context.schedule_maintenance(
            starts_in_minutes=starts_in_minutes,
            actor_id=admin_actor_id(request),
            reason=reason,
            idempotency_key=idempotency_key,
        )
        delivery = (
            await fanout_maintenance_state(context, state)
            if changed
            else {
                "realtime_delivered_clients": 0,
                "snapshot_only_clients": 0,
                "unsupported_versions": [],
                "disconnected_versions": [],
                "failed_deliveries": [],
                "per_version": {},
                "process_scope": "single_process",
            }
        )
        deadline = await arm_maintenance_deadline(context, state)
        return JSONResponse(
            {
                "Success": True,
                **state,
                **delivery,
                "deadline_enforcement": deadline,
                "capabilities": maintenance_api_capabilities(context),
                "idempotent_replay": not changed,
            }
        )

    @app.get("/admin/maintenance")
    async def admin_get_maintenance(request: Request) -> JSONResponse:
        await require_operator_request(request)
        return JSONResponse(
            {
                "Success": True,
                **context.get_maintenance_state(),
                "capabilities": maintenance_api_capabilities(context),
            }
        )

    @app.delete("/admin/maintenance")
    async def admin_cancel_maintenance(request: Request) -> JSONResponse:
        await require_operator_request(request)
        payload: dict[str, Any] = {}
        raw_body = await request.body()
        if raw_body:
            try:
                decoded = json.loads(raw_body)
            except Exception as exc:
                raise HTTPException(status_code=400, detail="Maintenance payload must be JSON.") from exc
            if not isinstance(decoded, dict):
                raise HTTPException(status_code=400, detail="Maintenance payload must be a JSON object.")
            payload = decoded
        reason_value = payload.get("reason", request.query_params.get("reason"))
        reason = str(reason_value).strip()[:2000] if reason_value is not None else None
        idempotency_value = payload.get(
            "idempotency_key",
            request.query_params.get("idempotency_key"),
        )
        idempotency_key = (
            str(idempotency_value).strip()[:200]
            if idempotency_value is not None
            else None
        )
        if idempotency_value is not None and not idempotency_key:
            raise HTTPException(status_code=422, detail="idempotency_key cannot be empty.")
        state, changed = context.cancel_maintenance(
            actor_id=admin_actor_id(request),
            reason=reason,
            idempotency_key=idempotency_key,
        )
        delivery = (
            await fanout_maintenance_state(context, state, cancellation=True)
            if changed
            else {
                "realtime_delivered_clients": 0,
                "snapshot_only_clients": 0,
                "unsupported_versions": [],
                "disconnected_versions": [],
                "failed_deliveries": [],
                "per_version": {},
                "process_scope": "single_process",
            }
        )
        deadline = await arm_maintenance_deadline(context, state)
        return JSONResponse(
            {
                "Success": True,
                **state,
                **delivery,
                "deadline_enforcement": deadline,
                "capabilities": maintenance_api_capabilities(context),
                "idempotent_replay": not changed,
            }
        )

    @app.get("/admin/motd")
    async def admin_get_motd(request: Request) -> JSONResponse:
        await require_operator_request(request)
        return JSONResponse(
            {
                "Success": True,
                "Scope": "shared",
                "Key": "motd",
                "MessageOfTheDay": context.get_motd(),
            }
        )

    @app.post("/admin/motd")
    async def admin_set_motd(request: Request) -> JSONResponse:
        await require_operator_request(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Admin MOTD payload must be JSON.") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Admin MOTD payload must be a JSON object.")

        raw_message = payload.get("message", payload.get("MessageOfTheDay", payload.get("motd", "")))
        if raw_message is None:
            raw_message = ""
        message = str(raw_message)
        if len(message.encode("utf-8")) > 8192:
            raise HTTPException(status_code=413, detail="MOTD is too large.")

        context.set_shared_motd(message)
        return JSONResponse({"Success": True, "Scope": "shared", "Key": "motd"})

    @app.post("/admin/players/administrator")
    async def admin_set_player_administrator(request: Request) -> JSONResponse:
        await require_operator_request(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail="Administrator payload must be JSON.",
            ) from exc
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400,
                detail="Administrator payload must be a JSON object.",
            )

        username = str(payload.get("username") or "").strip()
        if not username:
            raise HTTPException(status_code=400, detail="username is required.")
        administrator = payload.get("administrator", True)
        if not isinstance(administrator, bool):
            raise HTTPException(
                status_code=400,
                detail="administrator must be a JSON boolean.",
            )

        with context.db.connection() as conn:
            rows = conn.execute(
                """
                SELECT player_id, username, display_name, is_coach
                FROM players
                WHERE username = ? COLLATE NOCASE
                ORDER BY CASE WHEN username = ? THEN 0 ELSE 1 END, created_at
                """,
                (username, username),
            ).fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="Player not found.")
        exact_rows = [row for row in rows if str(row["username"]) == username]
        if len(rows) > 1 and len(exact_rows) != 1:
            raise HTTPException(
                status_code=409,
                detail="Username is ambiguous; use its exact casing.",
            )
        player = exact_rows[0] if exact_rows else rows[0]
        if bool(player["is_coach"]):
            raise HTTPException(
                status_code=409,
                detail="Coach already has canonical system-room authority.",
            )

        context.set_player_administrator(player["player_id"], administrator)
        return JSONResponse(
            {
                "Success": True,
                "PlayerId": str(player["player_id"]),
                "Username": str(player["username"]),
                "DisplayName": str(player["display_name"] or player["username"]),
                "Administrator": administrator,
            }
        )

    @app.get(
        "/admin/bug-reports",
        dependencies=[Depends(require_documented_admin)],
        tags=["bug-reports"],
    )
    async def admin_list_bug_reports(request: Request) -> JSONResponse:
        status_filter = str(request.query_params.get("status") or "").strip()
        if status_filter and status_filter not in {"open", "dismissed"}:
            raise HTTPException(status_code=422, detail="Unsupported bug-report status.")
        query = str(request.query_params.get("q") or "").strip()[:500]
        group_id = str(request.query_params.get("group_id") or "").strip()[:100]
        try:
            limit = int(request.query_params.get("limit", "100"))
            offset = int(request.query_params.get("offset", "0"))
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail="limit and offset must be integers.",
            ) from exc
        reports, total = list_bug_reports(
            db,
            status_filter=status_filter or None,
            query=query or None,
            group_id=group_id or None,
            limit=limit,
            offset=offset,
        )
        with db.connection() as conn:
            open_count = int(
                conn.execute(
                    "SELECT COUNT(*) AS total FROM bug_reports WHERE status = 'open'"
                ).fetchone()["total"]
            )
        return JSONResponse(
            {
                "Success": True,
                "Reports": reports,
                "Groups": list_bug_report_groups(db),
                "Total": total,
                "OpenCount": open_count,
                "Limit": max(1, min(limit, 200)),
                "Offset": max(0, offset),
            }
        )

    @app.get(
        "/admin/bug-reports/groups",
        dependencies=[Depends(require_documented_admin)],
        tags=["bug-reports"],
    )
    async def admin_list_bug_report_groups(_: Request) -> JSONResponse:
        return JSONResponse(
            {
                "Success": True,
                "Groups": list_bug_report_groups(db),
            }
        )

    @app.post(
        "/admin/bug-reports/group",
        dependencies=[Depends(require_documented_admin)],
        tags=["bug-reports"],
    )
    async def admin_group_bug_reports(
        request: Request,
        payload: BugReportGroupRequest,
    ) -> JSONResponse:
        reason = payload.reason.strip()
        if not reason:
            raise HTTPException(status_code=422, detail="reason cannot be blank.")
        try:
            group, changed, replay = group_bug_reports(
                db,
                report_ids=payload.report_ids,
                group_id=payload.group_id,
                title=payload.title,
                actor_id=admin_actor_id(request),
                reason=reason,
                idempotency_key=payload.idempotency_key,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=404,
                detail=f"Bug report or group not found: {exc.args[0]}",
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return JSONResponse(
            {
                "Success": True,
                "Group": group,
                "Changed": changed,
                "IdempotentReplay": replay,
            }
        )

    @app.get(
        "/admin/bug-reports/{report_id}",
        dependencies=[Depends(require_documented_admin)],
        tags=["bug-reports"],
    )
    async def admin_get_bug_report(report_id: str) -> JSONResponse:
        report = get_bug_report(db, report_id)
        if report is None:
            raise HTTPException(status_code=404, detail="Bug report not found.")
        return JSONResponse({"Success": True, "Report": report})

    @app.get(
        "/admin/bug-reports/{report_id}/attachments/{kind}",
        dependencies=[Depends(require_documented_admin)],
        tags=["bug-reports"],
    )
    async def admin_get_bug_report_attachment(
        report_id: str,
        kind: str,
    ) -> Response:
        if kind not in {"screenshot", "log"}:
            raise HTTPException(status_code=404, detail="Bug-report attachment not found.")
        attachment = get_bug_report_attachment(db, report_id, kind)
        if attachment is None:
            raise HTTPException(status_code=404, detail="Bug-report attachment not found.")
        _stored_name, data = attachment
        if kind == "log":
            media_type = "text/plain; charset=utf-8"
            filename = f"bug-report-{report_id}-output.txt"
        elif data.startswith(b"\x89PNG\r\n\x1a\n"):
            media_type = "image/png"
            filename = f"bug-report-{report_id}-screenshot.png"
        elif data.startswith(b"\xff\xd8\xff"):
            media_type = "image/jpeg"
            filename = f"bug-report-{report_id}-screenshot.jpg"
        else:
            media_type = "application/octet-stream"
            filename = f"bug-report-{report_id}-screenshot.bin"
        return Response(
            content=data,
            media_type=media_type,
            headers={
                "Cache-Control": "no-store",
                "Content-Disposition": f'inline; filename="{filename}"',
                "X-Content-Type-Options": "nosniff",
            },
        )

    @app.post(
        "/admin/bug-reports/{report_id}/dismiss",
        dependencies=[Depends(require_documented_admin)],
        tags=["bug-reports"],
    )
    async def admin_dismiss_bug_report(
        report_id: str,
        request: Request,
        payload: BugReportDismissRequest,
    ) -> JSONResponse:
        reason = payload.reason.strip()
        if not reason:
            raise HTTPException(status_code=422, detail="reason cannot be blank.")
        try:
            report, changed, replay = dismiss_bug_report(
                db,
                report_id=report_id,
                actor_id=admin_actor_id(request),
                reason=reason,
                idempotency_key=payload.idempotency_key,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Bug report not found.") from exc
        return JSONResponse(
            {
                "Success": True,
                "Report": report,
                "Changed": changed,
                "IdempotentReplay": replay,
            }
        )

    @app.post(
        "/admin/bug-reports/{report_id}/ungroup",
        dependencies=[Depends(require_documented_admin)],
        tags=["bug-reports"],
    )
    async def admin_ungroup_bug_report(
        report_id: str,
        request: Request,
        payload: BugReportUngroupRequest,
    ) -> JSONResponse:
        reason = payload.reason.strip()
        if not reason:
            raise HTTPException(status_code=422, detail="reason cannot be blank.")
        try:
            report, changed, replay = ungroup_bug_report(
                db,
                report_id=report_id,
                actor_id=admin_actor_id(request),
                reason=reason,
                idempotency_key=payload.idempotency_key,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Bug report not found.") from exc
        return JSONResponse(
            {
                "Success": True,
                "Report": report,
                "Changed": changed,
                "IdempotentReplay": replay,
            }
        )

    @app.get(
        "/admin/moderation/cases",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_list_moderation_cases(request: Request) -> JSONResponse:
        await require_operator_request(request)
        state_value = request.query_params.get("state")
        state_filter = str(state_value).strip() if state_value is not None else None
        try:
            limit = int(request.query_params.get("limit", "100"))
            offset = int(request.query_params.get("offset", "0"))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="limit and offset must be integers.") from exc
        limit = max(1, min(limit, 200))
        offset = max(0, offset)
        cases = moderation_service.list_cases(
            db,
            state=state_filter,
            limit=limit,
            offset=offset,
        )
        severity_for_category = {
            "credible_threat": "critical",
            "underage_safety": "critical",
            "sexual_misconduct": "high",
            "discrimination": "high",
            "harassment": "high",
            "cheating": "medium",
            "exploitation": "medium",
            "afk": "low",
        }
        with db.connection() as conn:
            for case in cases:
                enrich_admin_moderation_case(db, case)
                metrics = conn.execute(
                    """
                    SELECT
                        COUNT(DISTINCT CASE
                            WHEN counts_toward_case_score = 1
                            THEN reporter_player_id END
                        ) AS independent_reporters,
                        SUM(CASE WHEN duplicate_of IS NOT NULL THEN 1 ELSE 0 END)
                            AS duplicate_reports
                    FROM moderation_reports
                    WHERE case_id = ?
                    """,
                    (case["case_id"],),
                ).fetchone()
                active_controls = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM (
                        SELECT sanction_id AS id
                        FROM moderation_sanctions
                        WHERE case_id = ? AND active = 1
                        UNION ALL
                        SELECT control_id AS id
                        FROM moderation_content_controls
                        WHERE case_id = ? AND active = 1
                    )
                    """,
                    (case["case_id"], case["case_id"]),
                ).fetchone()
                case["independent_reporters"] = int(
                    metrics["independent_reporters"] or 0
                )
                case["duplicate_reports"] = int(metrics["duplicate_reports"] or 0)
                case["active_controls"] = int(active_controls["count"] or 0)
                case["severity"] = severity_for_category.get(
                    str(case["canonical_category"]),
                    "normal",
                )
        return JSONResponse(
            {
                "Success": True,
                "Cases": cases,
                "Limit": limit,
                "Offset": offset,
            }
        )

    @app.get(
        "/admin/moderation/cases/{case_id}",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_get_moderation_case(case_id: str, request: Request) -> JSONResponse:
        await require_operator_request(request)
        case = moderation_service.get_case(db, case_id)
        if case is None:
            raise HTTPException(status_code=404, detail="Moderation case not found.")
        enrich_admin_moderation_case(db, case)
        case["allowed_actions"] = admin_allowed_actions(
            db,
            target_type=str(case["target_type"]),
            target_id=str(case["target_id"]),
            case=case,
        )
        return JSONResponse({"Success": True, "Case": case})

    @app.get(
        "/admin/moderation/cases/{case_id}/evidence",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_get_moderation_evidence(case_id: str, request: Request) -> JSONResponse:
        await require_operator_request(request)
        case = moderation_service.get_case(db, case_id)
        if case is None:
            raise HTTPException(status_code=404, detail="Moderation case not found.")
        include_raw = str(request.query_params.get("include_raw") or "").strip().casefold() in {
            "1",
            "true",
            "yes",
        }
        if include_raw and getattr(request.state, "admin_session", None) is not None:
            raise HTTPException(
                status_code=405,
                detail="Browser sessions must use the CSRF-protected evidence reveal route.",
            )
        evidence = moderation_service.get_case_evidence(
            db,
            case_id,
            include_raw=include_raw,
        )
        if include_raw and evidence:
            moderation_service.record_evidence_access(
                db,
                case_id=case_id,
                actor_id=admin_actor_id(request),
                evidence_ids=[str(item["evidence_id"]) for item in evidence],
            )
        return JSONResponse(
            {
                "Success": True,
                "CaseId": case_id,
                "RawIncluded": include_raw,
                "Evidence": evidence,
            }
        )

    @app.post(
        "/admin/moderation/cases/{case_id}/evidence/reveal",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_reveal_moderation_evidence(
        case_id: str,
        request: Request,
    ) -> JSONResponse:
        await require_operator_request(request, write=True)
        case = moderation_service.get_case(db, case_id)
        if case is None:
            raise HTTPException(status_code=404, detail="Moderation case not found.")
        evidence = moderation_service.get_case_evidence(
            db,
            case_id,
            include_raw=True,
        )
        if evidence:
            moderation_service.record_evidence_access(
                db,
                case_id=case_id,
                actor_id=admin_actor_id(request),
                evidence_ids=[str(item["evidence_id"]) for item in evidence],
            )
        return JSONResponse(
            {
                "Success": True,
                "CaseId": case_id,
                "RawIncluded": True,
                "Evidence": evidence,
            }
        )

    @app.get(
        "/admin/moderation/targets/search",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_search_moderation_targets(request: Request) -> JSONResponse:
        await require_operator_request(request)
        query = str(request.query_params.get("q") or "").strip()
        try:
            limit = int(request.query_params.get("limit", "50"))
            results = search_admin_targets(db, query, limit=limit)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return JSONResponse({"Success": True, "Query": query, "Results": results})

    @app.get(
        "/admin/moderation/players/{player_id}",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_get_moderation_player(
        player_id: str,
        request: Request,
    ) -> JSONResponse:
        await require_operator_request(request)
        player = admin_player_detail(db, player_id)
        if player is None:
            raise HTTPException(status_code=404, detail="Player not found.")
        return JSONResponse({"Success": True, "Player": player})

    @app.get(
        "/admin/moderation/content/{target_type}/{target_id}",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_get_moderation_content(
        target_type: str,
        target_id: str,
        request: Request,
    ) -> JSONResponse:
        await require_operator_request(request)
        if target_type not in {"room", "invention", "player_event", "image"}:
            raise HTTPException(status_code=422, detail="Unsupported content target type.")
        content = admin_content_detail(
            db,
            target_type=target_type,
            target_id=target_id,
        )
        if content is None:
            raise HTTPException(status_code=404, detail="Content not found.")
        return JSONResponse({"Success": True, "Content": content})

    @app.get(
        "/admin/moderation/audit",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_get_moderation_audit(request: Request) -> JSONResponse:
        await require_operator_request(request)
        try:
            limit = int(request.query_params.get("limit", "100"))
            offset = int(request.query_params.get("offset", "0"))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="limit and offset must be integers.") from exc
        action = str(request.query_params.get("action") or "").strip() or None
        target = str(request.query_params.get("target") or "").strip() or None
        rows = list_admin_audit(
            db,
            action=action,
            target=target,
            limit=limit,
            offset=offset,
        )
        return JSONResponse(
            {
                "Success": True,
                "Audit": rows,
                "Limit": max(1, min(limit, 200)),
                "Offset": max(0, offset),
            }
        )

    @app.get(
        "/admin/operations/timed-content",
        dependencies=[Depends(require_documented_admin)],
        tags=["operations"],
    )
    async def admin_get_timed_content_status(request: Request) -> JSONResponse:
        await require_operator_request(request)
        with db.connection() as conn:
            rows = conn.execute(
                """
                SELECT s.schedule_key, s.model, s.revision, s.catalog_revision,
                       s.active, s.updated_at,
                       p.period_id, p.starts_at_utc, p.ends_at_utc,
                       p.materialized_at
                FROM timed_content_schedules AS s
                LEFT JOIN timed_content_periods AS p
                  ON p.schedule_key = s.schedule_key
                 AND p.period_id = (
                     SELECT current.period_id
                     FROM timed_content_periods AS current
                     WHERE current.schedule_key = s.schedule_key
                     ORDER BY current.period_index DESC
                     LIMIT 1
                 )
                ORDER BY s.schedule_key
                """
            ).fetchall()
        schedules = [dict(row) for row in rows]
        for schedule in schedules:
            schedule["active"] = bool(schedule["active"])
        return JSONResponse({"Success": True, "Schedules": schedules})

    @app.post(
        "/admin/operations/timed-content/reconcile",
        dependencies=[Depends(require_documented_admin)],
        tags=["operations"],
    )
    async def admin_reconcile_timed_content(request: Request) -> JSONResponse:
        await require_operator_request(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Reconcile payload must be JSON.") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Reconcile payload must be a JSON object.")
        reason = str(payload.get("reason") or "").strip()
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        if not reason:
            raise HTTPException(status_code=422, detail="reason is required.")
        if len(idempotency_key) < 8:
            raise HTTPException(status_code=422, detail="idempotency_key is required.")
        existing: sqlite3.Row | None = None
        with db.connection() as conn:
            existing = conn.execute(
                "SELECT * FROM moderation_actions WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        if existing is not None:
            return JSONResponse(
                {
                    "Success": True,
                    "IdempotentReplay": True,
                    "Reconciled": json.loads(existing["metadata_json"]).get(
                        "reconciled",
                        [],
                    ),
                }
            )
        reconciled = timed_content.reconcile_due_timed_content(
            db,
            now_utc=utc_datetime_now(),
        )
        moderation_service.append_operator_action(
            db,
            case_id=None,
            target_type="server",
            target_id="timed_content",
            actor_id=admin_actor_id(request),
            action="reconcile_timed_content",
            previous_state=None,
            new_state=None,
            reason=reason,
            idempotency_key=idempotency_key,
            metadata={"reconciled": reconciled},
        )
        return JSONResponse(
            {
                "Success": True,
                "IdempotentReplay": False,
                "Reconciled": reconciled,
            }
        )

    @app.post(
        "/admin/moderation/actions",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_apply_moderation_action(
        request: Request,
        payload: ModerationActionRequest,
    ) -> JSONResponse:
        await require_operator_request(request)
        target_type = payload.target_type.strip()
        target_id = payload.target_id.strip()
        action = payload.action.strip()
        reason = payload.reason.strip()
        case: dict[str, Any] | None = None
        if payload.case_id:
            case = moderation_service.get_case(db, payload.case_id)
            if case is None:
                raise HTTPException(status_code=404, detail="Moderation case not found.")
            if (
                str(case["target_type"]) != target_type
                or str(case["target_id"]) != target_id
            ):
                raise HTTPException(
                    status_code=409,
                    detail="The selected case does not match the requested target.",
                )
        elif target_type == "player":
            if admin_player_detail(db, target_id) is None:
                raise HTTPException(status_code=404, detail="Player not found.")
        else:
            case = find_latest_moderation_case(
                db,
                target_type=target_type,
                target_id=target_id,
            )

        allowed = admin_allowed_actions(
            db,
            target_type=target_type,
            target_id=target_id,
            case=case,
        )
        allowed_by_name = {item["name"]: item for item in allowed}
        if action not in allowed_by_name:
            raise HTTPException(
                status_code=422,
                detail="This action is not allowed for the selected target state.",
            )
        confirmation_phrase = allowed_by_name[action].get("confirmation_phrase")
        if confirmation_phrase is not None and not hmac.compare_digest(
            str(payload.confirmation or ""),
            str(confirmation_phrase),
        ):
            raise HTTPException(
                status_code=422,
                detail=f"Type the exact confirmation phrase: {confirmation_phrase}",
            )
        with db.connection() as conn:
            replay = conn.execute(
                "SELECT * FROM moderation_actions WHERE idempotency_key = ?",
                (payload.idempotency_key,),
            ).fetchone()
        if replay is not None:
            return JSONResponse(
                {
                    "Success": True,
                    "IdempotentReplay": True,
                    "Action": dict(replay),
                }
            )

        actor_id = admin_actor_id(request)
        case_id = str(case["case_id"]) if case else None
        room_enforcement: dict[str, Any] | None = None
        try:
            if action == "observe":
                moderation_service.transition_case(
                    db,
                    case_id=str(case_id),
                    action="observe",
                    actor_id=actor_id,
                    reason=reason,
                    idempotency_key=payload.idempotency_key,
                )
            elif action == "dismiss_case":
                moderation_service.transition_case(
                    db,
                    case_id=str(case_id),
                    action="dismiss",
                    actor_id=actor_id,
                    reason=reason,
                    idempotency_key=payload.idempotency_key,
                )
            elif action == "timeout":
                if payload.duration_seconds is None:
                    raise ValueError("A timeout duration is required.")
                if case is not None:
                    moderation_service.transition_case(
                        db,
                        case_id=str(case_id),
                        action="timeout",
                        actor_id=actor_id,
                        reason=reason,
                        duration_seconds=payload.duration_seconds,
                        idempotency_key=payload.idempotency_key,
                    )
                else:
                    now = utc_now()
                    expires_at = (
                        datetime.now(timezone.utc)
                        + timedelta(seconds=payload.duration_seconds)
                    ).isoformat(timespec="seconds").replace("+00:00", "Z")
                    action_id = moderation_service.append_operator_action(
                        db,
                        case_id=None,
                        target_type="player",
                        target_id=target_id,
                        actor_id=actor_id,
                        action="timeout",
                        previous_state="normal",
                        new_state="timed_out",
                        reason=reason,
                        duration_seconds=payload.duration_seconds,
                        idempotency_key=payload.idempotency_key,
                    )
                    with db.transaction() as conn:
                        conn.execute(
                            """
                            INSERT INTO moderation_sanctions(
                                sanction_id, case_id, target_player_id,
                                sanction_type, scope, active, starts_at,
                                expires_at, reason, created_by,
                                reversed_by_action_id, created_at, updated_at
                            )
                            VALUES (?, NULL, ?, 'timeout', 'account', 1, ?, ?, ?, ?, NULL, ?, ?)
                            """,
                            (
                                str(uuid.uuid4()),
                                target_id,
                                now,
                                expires_at,
                                reason,
                                actor_id,
                                now,
                                now,
                            ),
                        )
            elif action == "permanent_account_ban":
                if case is not None:
                    moderation_service.transition_case(
                        db,
                        case_id=str(case_id),
                        action="ban",
                        actor_id=actor_id,
                        reason=reason,
                        idempotency_key=payload.idempotency_key,
                    )
                else:
                    moderation_service.append_operator_action(
                        db,
                        case_id=None,
                        target_type="player",
                        target_id=target_id,
                        actor_id=actor_id,
                        action="ban",
                        previous_state="normal",
                        new_state="banned",
                        reason=reason,
                        idempotency_key=payload.idempotency_key,
                    )
                context.create_player_ban(
                    target_id,
                    reason=reason,
                    created_by=actor_id,
                    case_id=case_id,
                )
            elif action in {
                "restrict_invention_publishing",
                "restore_invention_publishing",
            }:
                moderation_service.set_player_scope_restriction(
                    db,
                    player_id=target_id,
                    scope="invention_publishing",
                    restrict=action == "restrict_invention_publishing",
                    actor_id=actor_id,
                    reason=reason,
                    case_id=case_id,
                    idempotency_key=payload.idempotency_key,
                )
            elif action == "quarantine_content":
                if case is None:
                    raise ValueError("Content quarantine requires a moderation case.")
                source_versions = [
                    str(report.get("source_version") or "").strip()
                    for report in reversed(case.get("reports", []))
                    if isinstance(report, dict)
                    and str(report.get("source_version") or "").strip()
                ]
                enforced = False
                for source_version in dict.fromkeys(source_versions):
                    module = load_version_module(settings, source_version)
                    validator = getattr(module, "validate_moderation_action", None)
                    if validator is None:
                        continue
                    validator(
                        action="quarantine",
                        target_type=target_type,
                        target_id=target_id,
                        context=context,
                    )
                    enforced = True
                    break
                if not enforced:
                    raise NotImplementedError(
                        "This content type has no implemented quarantine adapter."
                    )
                moderation_service.transition_case(
                    db,
                    case_id=str(case_id),
                    action="quarantine",
                    actor_id=actor_id,
                    reason=reason,
                    idempotency_key=payload.idempotency_key,
                )
            elif action == "restore_content":
                if case is None:
                    raise ValueError("Content restore requires a moderation case.")
                moderation_service.reverse_case_action(
                    db,
                    case_id=str(case_id),
                    actor_id=actor_id,
                    reason=reason,
                    idempotency_key=payload.idempotency_key,
                    reversal_action="restore",
                )
            else:
                raise ValueError("Unsupported moderation action.")
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Target not found.") from exc
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        if target_type == "player" and action in {
            "timeout",
            "permanent_account_ban",
        }:
            room_enforcement = await enforce_player_room_lock(
                context,
                player_id=target_id,
                sanction_type=(
                    "ban" if action == "permanent_account_ban" else "timeout"
                ),
                reason=reason,
                duration_seconds=payload.duration_seconds,
            )

        with db.connection() as conn:
            action_row = conn.execute(
                "SELECT * FROM moderation_actions WHERE idempotency_key = ?",
                (payload.idempotency_key,),
            ).fetchone()
        refreshed_case = (
            moderation_service.get_case(db, str(case_id))
            if case_id is not None
            else None
        )
        if refreshed_case is not None:
            refreshed_case["allowed_actions"] = admin_allowed_actions(
                db,
                target_type=target_type,
                target_id=target_id,
                case=refreshed_case,
            )
        return JSONResponse(
            {
                "Success": True,
                "IdempotentReplay": False,
                "Action": dict(action_row) if action_row is not None else None,
                "Case": refreshed_case,
                "RoomEnforcement": room_enforcement,
            }
        )

    @app.post(
        "/admin/moderation/actions/{action_id}/reverse",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_reverse_moderation_action(
        action_id: str,
        request: Request,
        payload: ModerationReasonRequest,
    ) -> JSONResponse:
        await require_operator_request(request)
        with db.connection() as conn:
            original = conn.execute(
                "SELECT * FROM moderation_actions WHERE action_id = ?",
                (action_id,),
            ).fetchone()
        if original is None:
            raise HTTPException(status_code=404, detail="Moderation action not found.")
        if str(original["action"]) in {
            "restrict_invention_publishing",
        }:
            try:
                reversed_action = moderation_service.set_player_scope_restriction(
                    db,
                    player_id=str(original["target_id"]),
                    scope="invention_publishing",
                    restrict=False,
                    actor_id=admin_actor_id(request),
                    reason=payload.reason,
                    case_id=str(original["case_id"]) if original["case_id"] else None,
                    idempotency_key=payload.idempotency_key,
                )
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            return JSONResponse({"Success": True, "Action": reversed_action})
        if original["case_id"]:
            try:
                moderation_service.reverse_case_action(
                    db,
                    case_id=str(original["case_id"]),
                    actor_id=admin_actor_id(request),
                    reason=payload.reason,
                    action_id=action_id,
                    idempotency_key=payload.idempotency_key,
                )
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            return JSONResponse(
                {
                    "Success": True,
                    "Case": moderation_service.get_case(
                        db,
                        str(original["case_id"]),
                    ),
                }
            )
        if str(original["action"]) not in {"timeout", "ban"}:
            raise HTTPException(status_code=422, detail="This action is not reversible.")
        now = utc_now()
        reversal_id = moderation_service.append_operator_action(
            db,
            case_id=None,
            target_type=str(original["target_type"]),
            target_id=str(original["target_id"]),
            actor_id=admin_actor_id(request),
            action="reverse",
            previous_state=str(original["new_state"] or ""),
            new_state=str(original["previous_state"] or "normal"),
            reason=payload.reason,
            idempotency_key=payload.idempotency_key,
            reverses_action_id=action_id,
            metadata={"reversed_action": str(original["action"])},
        )
        with db.transaction() as conn:
            conn.execute(
                """
                UPDATE moderation_sanctions
                SET active = 0, reversed_by_action_id = ?, updated_at = ?
                WHERE target_player_id = ?
                  AND sanction_type = ?
                  AND active = 1
                """,
                (
                    reversal_id,
                    now,
                    str(original["target_id"]),
                    str(original["action"]),
                ),
            )
        if str(original["action"]) == "ban":
            context.unban_player(str(original["target_id"]))
        return JSONResponse({"Success": True, "ActionId": reversal_id})

    async def apply_case_transition(
        case_id: str,
        request: Request,
        action: str,
    ) -> JSONResponse:
        await require_operator_request(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Moderation action payload must be JSON.") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Moderation action payload must be a JSON object.")
        reason = str(payload.get("reason") or "").strip()
        if not reason:
            raise HTTPException(status_code=422, detail="reason is required.")
        duration_value = payload.get("duration_seconds")
        if isinstance(duration_value, bool):
            raise HTTPException(status_code=422, detail="duration_seconds must be an integer.")
        if duration_value is not None and not isinstance(duration_value, int):
            raise HTTPException(status_code=422, detail="duration_seconds must be an integer.")
        idempotency_value = payload.get("idempotency_key")
        idempotency_key = (
            str(idempotency_value).strip()[:200]
            if idempotency_value is not None
            else None
        )
        existing_case = moderation_service.get_case(db, case_id)
        if existing_case is None:
            raise HTTPException(status_code=404, detail="Moderation case not found.")
        if action == "quarantine":
            source_versions = [
                str(report.get("source_version") or "").strip()
                for report in reversed(existing_case.get("reports", []))
                if isinstance(report, dict)
                and str(report.get("source_version") or "").strip()
            ]
            enforced = False
            for source_version in dict.fromkeys(source_versions):
                module = load_version_module(settings, source_version)
                validator = getattr(module, "validate_moderation_action", None)
                if validator is None:
                    continue
                try:
                    validator(
                        action=action,
                        target_type=str(existing_case["target_type"]),
                        target_id=str(existing_case["target_id"]),
                        context=context,
                    )
                except ValueError as exc:
                    raise HTTPException(status_code=422, detail=str(exc)) from exc
                enforced = True
                break
            if not enforced:
                raise HTTPException(
                    status_code=501,
                    detail=(
                        "This reported content type has no implemented "
                        "quarantine enforcement adapter."
                    ),
                )
        try:
            moderation_service.transition_case(
                db,
                case_id=case_id,
                action=action,
                actor_id=admin_actor_id(request),
                reason=reason,
                duration_seconds=duration_value,
                idempotency_key=idempotency_key,
            )
            if action == "ban":
                context.create_player_ban(
                    str(existing_case["target_id"]),
                    reason=reason,
                    created_by=admin_actor_id(request),
                    case_id=case_id,
                )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Moderation case not found.") from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        room_enforcement = None
        if (
            str(existing_case["target_type"]) == "player"
            and action in {"timeout", "ban"}
        ):
            room_enforcement = await enforce_player_room_lock(
                context,
                player_id=str(existing_case["target_id"]),
                sanction_type=action,
                reason=reason,
                duration_seconds=duration_value,
            )
        case = moderation_service.get_case(db, case_id)
        return JSONResponse(
            {
                "Success": True,
                "Case": case,
                "RoomEnforcement": room_enforcement,
            }
        )

    @app.post(
        "/admin/moderation/cases/{case_id}/assign",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_assign_moderation_case(
        case_id: str,
        request: Request,
        _: ModerationAssignmentRequest,
    ) -> JSONResponse:
        await require_operator_request(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail="Moderation assignment payload must be JSON.",
            ) from exc
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400,
                detail="Moderation assignment payload must be a JSON object.",
            )
        reason = str(payload.get("reason") or "").strip()
        if not reason:
            raise HTTPException(status_code=422, detail="reason is required.")
        assigned_value = payload.get("assigned_to")
        assigned_to = (
            str(assigned_value).strip()[:200]
            if assigned_value is not None
            else None
        )
        idempotency_value = payload.get("idempotency_key")
        idempotency_key = (
            str(idempotency_value).strip()[:200]
            if idempotency_value is not None
            else None
        )
        try:
            moderation_service.assign_case(
                db,
                case_id=case_id,
                actor_id=admin_actor_id(request),
                assigned_to=assigned_to,
                reason=reason,
                idempotency_key=idempotency_key,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Moderation case not found.") from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return JSONResponse(
            {
                "Success": True,
                "Case": moderation_service.get_case(db, case_id),
            }
        )

    @app.post(
        "/admin/moderation/cases/{case_id}/observe",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_observe_moderation_case(
        case_id: str,
        request: Request,
        _: ModerationReasonRequest,
    ) -> JSONResponse:
        return await apply_case_transition(case_id, request, "observe")

    @app.post(
        "/admin/moderation/cases/{case_id}/restrict",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_restrict_moderation_case(
        case_id: str,
        request: Request,
        _: ModerationReasonRequest,
    ) -> JSONResponse:
        return await apply_case_transition(case_id, request, "restrict")

    @app.post(
        "/admin/moderation/cases/{case_id}/timeout",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_timeout_moderation_case(
        case_id: str,
        request: Request,
        _: ModerationTimeoutRequest,
    ) -> JSONResponse:
        return await apply_case_transition(case_id, request, "timeout")

    @app.post(
        "/admin/moderation/cases/{case_id}/quarantine",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_quarantine_moderation_case(
        case_id: str,
        request: Request,
        _: ModerationReasonRequest,
    ) -> JSONResponse:
        return await apply_case_transition(case_id, request, "quarantine")

    @app.post(
        "/admin/moderation/cases/{case_id}/dismiss",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_dismiss_moderation_case(
        case_id: str,
        request: Request,
        _: ModerationReasonRequest,
    ) -> JSONResponse:
        return await apply_case_transition(case_id, request, "dismiss")

    @app.post(
        "/admin/moderation/cases/{case_id}/ban",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_ban_moderation_case(
        case_id: str,
        request: Request,
        _: ModerationReasonRequest,
    ) -> JSONResponse:
        return await apply_case_transition(case_id, request, "ban")

    @app.post(
        "/admin/moderation/cases/{case_id}/reverse",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_reverse_moderation_case(
        case_id: str,
        request: Request,
        _: ModerationReversalRequest,
    ) -> JSONResponse:
        await require_operator_request(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail="Moderation reversal payload must be JSON.",
            ) from exc
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400,
                detail="Moderation reversal payload must be a JSON object.",
            )
        reason = str(payload.get("reason") or "").strip()
        if not reason:
            raise HTTPException(status_code=422, detail="reason is required.")
        action_value = payload.get("action_id")
        action_id = str(action_value).strip() if action_value is not None else None
        idempotency_value = payload.get("idempotency_key")
        idempotency_key = (
            str(idempotency_value).strip()[:200]
            if idempotency_value is not None
            else None
        )
        try:
            moderation_service.reverse_case_action(
                db,
                case_id=case_id,
                actor_id=admin_actor_id(request),
                reason=reason,
                action_id=action_id,
                idempotency_key=idempotency_key,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Moderation case not found.") from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return JSONResponse(
            {
                "Success": True,
                "Case": moderation_service.get_case(db, case_id),
            }
        )

    @app.post(
        "/admin/moderation/cases/{case_id}/restore",
        dependencies=[Depends(require_documented_admin)],
        tags=["moderation"],
    )
    async def admin_restore_moderation_case(
        case_id: str,
        request: Request,
        _: ModerationReversalRequest,
    ) -> JSONResponse:
        await require_operator_request(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail="Moderation restore payload must be JSON.",
            ) from exc
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400,
                detail="Moderation restore payload must be a JSON object.",
            )
        reason = str(payload.get("reason") or "").strip()
        if not reason:
            raise HTTPException(status_code=422, detail="reason is required.")
        action_value = payload.get("action_id")
        action_id = str(action_value).strip() if action_value is not None else None
        idempotency_value = payload.get("idempotency_key")
        idempotency_key = (
            str(idempotency_value).strip()[:200]
            if idempotency_value is not None
            else None
        )
        try:
            moderation_service.reverse_case_action(
                db,
                case_id=case_id,
                actor_id=admin_actor_id(request),
                reason=reason,
                action_id=action_id,
                idempotency_key=idempotency_key,
                reversal_action="restore",
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Moderation case not found.") from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return JSONResponse(
            {
                "Success": True,
                "Case": moderation_service.get_case(db, case_id),
            }
        )

    @app.get("/admin/ban/status")
    async def admin_ban_status(request: Request) -> JSONResponse:
        await require_operator_request(request)
        params = request.query_params
        api_version = admin_api_version_from_payload(dict(params))
        username = str(params.get("username") or params.get("Username") or "").strip()
        display_name = str(params.get("display_name") or params.get("displayName") or params.get("DisplayName") or "").strip()
        canonical_player_id = str(
            params.get("canonical_player_id") or params.get("canonicalPlayerId") or params.get("player_uuid") or ""
        ).strip()
        player_id_value = str(params.get("player_id") or params.get("playerId") or params.get("PlayerId") or "").strip()
        if not canonical_player_id and re.fullmatch(r"[0-9a-fA-F-]{32,36}", player_id_value):
            canonical_player_id = player_id_value
            player_id_value = ""
        recnet_id = str(params.get("recnet_id") or params.get("recNetId") or params.get("recnetId") or "").strip()
        if not recnet_id and player_id_value:
            recnet_id = player_id_value
        platform = str(params.get("platform") or params.get("Platform") or "").strip()
        platform_id = str(params.get("platform_id") or params.get("platformId") or params.get("PlatformId") or "").strip()
        account_id = str(params.get("account_id") or params.get("accountId") or "").strip()
        ip = str(params.get("ip") or params.get("ipAddress") or params.get("ip_address") or "").strip()
        hardware_id = str(params.get("hardware_id") or params.get("hardwareId") or params.get("device_id") or params.get("deviceId") or "").strip()

        identities: list[tuple[str, Any]] = []
        if username:
            identities.append(("username_lower", username))
        if display_name:
            identities.append(("username_lower", display_name))
        if canonical_player_id:
            identities.append(("account_id", canonical_player_id))
        if account_id:
            identities.append(("account_id", account_id))
        if recnet_id:
            append_recnet_identity_pairs(identities, recnet_id, api_version)
        if platform_id:
            identities.append(("account_id", f"platform:{platform or 0}:{platform_id}"))
            if (platform or "0") == "0":
                identities.append(("account_id", f"steam:{platform_id}"))
        if ip:
            identities.append(("ip_hash", ip))
        if hardware_id:
            identities.append(("hardware_id_hash", hardware_id))
        if not identities:
            raise HTTPException(status_code=400, detail="Provide at least one player or identity field.")

        checked: list[dict[str, str]] = []
        active_bans: list[dict[str, Any]] = []
        with db.connection() as conn:
            for identity_type, value in identities:
                identity_hash = context.identity_hash(identity_type, value)
                if not identity_hash:
                    continue
                checked.append({"identity_type": identity_type, "value": str(value)})
                rows = conn.execute(
                    """
                    SELECT id, player_id, identity_type, reason, active, created_at, updated_at
                    FROM bans
                    WHERE identity_type = ?
                      AND identity_hash = ?
                      AND active = 1
                    ORDER BY updated_at DESC
                    """,
                    (identity_type, identity_hash),
                ).fetchall()
                for row in rows:
                    active_bans.append(
                        {
                            "id": row["id"],
                            "player_id": row["player_id"],
                            "identity_type": row["identity_type"],
                            "reason": row["reason"],
                            "created_at": row["created_at"],
                            "updated_at": row["updated_at"],
                        }
                    )

        return JSONResponse({"Success": True, "IsBanned": bool(active_bans), "CheckedIdentities": checked, "ActiveBans": active_bans})

    @app.post("/admin/ban")
    async def admin_ban(request: Request) -> JSONResponse:
        await require_operator_request(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Admin ban payload must be JSON.") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Admin ban payload must be a JSON object.")

        api_version = admin_api_version_from_payload(payload)
        reason = str(payload.get("reason") or payload.get("Reason") or "Banned by server operator.").strip()

        username = str(payload.get("username") or payload.get("Username") or "").strip()
        display_name = str(
            payload.get("display_name") or payload.get("displayName") or payload.get("DisplayName") or ""
        ).strip()
        canonical_player_id = str(
            payload.get("canonical_player_id")
            or payload.get("canonicalPlayerId")
            or payload.get("player_uuid")
            or ""
        ).strip()
        player_id_value = str(payload.get("player_id") or payload.get("playerId") or payload.get("PlayerId") or "").strip()
        if not canonical_player_id and re.fullmatch(r"[0-9a-fA-F-]{32,36}", player_id_value):
            canonical_player_id = player_id_value
            player_id_value = ""
        recnet_id = str(payload.get("recnet_id") or payload.get("recNetId") or payload.get("recnetId") or "").strip()
        if not recnet_id and player_id_value:
            recnet_id = player_id_value
        platform = str(payload.get("platform") or payload.get("Platform") or "").strip()
        platform_id = str(payload.get("platform_id") or payload.get("platformId") or payload.get("PlatformId") or "").strip()
        account_id = str(payload.get("account_id") or payload.get("accountId") or "").strip()
        ip = str(payload.get("ip") or payload.get("ipAddress") or payload.get("ip_address") or "").strip()
        hardware_id = str(
            payload.get("hardware_id") or payload.get("hardwareId") or payload.get("device_id") or payload.get("deviceId") or ""
        ).strip()
        allow_multiple = payload_truthy(payload, "allow_multiple", "allowMultiple", "AllowMultiple")
        has_strong_player_identifier = bool(
            username or canonical_player_id or recnet_id or platform_id or account_id or ip or hardware_id
        )

        identities: list[tuple[str, Any]] = []
        if username:
            identities.append(("username_lower", username))
        if display_name:
            identities.append(("username_lower", display_name))
        if canonical_player_id:
            identities.append(("account_id", canonical_player_id))
        if account_id:
            identities.append(("account_id", account_id))
        if recnet_id:
            append_recnet_identity_pairs(identities, recnet_id, api_version)
        if platform_id:
            platform_key = f"platform:{platform or 0}:{platform_id}"
            identities.append(("account_id", platform_key))
        if ip:
            identities.append(("ip_hash", ip))
        if hardware_id:
            identities.append(("hardware_id_hash", hardware_id))

        if not identities and not canonical_player_id:
            raise HTTPException(status_code=400, detail="Provide at least one player or identity field to ban.")
        if any(normalize_identity_value(value) == "coach" for identity_type, value in identities if identity_type == "username_lower"):
            raise HTTPException(status_code=403, detail="Coach cannot be banned.")

        matched: dict[str, sqlite3.Row] = {}
        with db.connection() as conn:
            if canonical_player_id:
                row = conn.execute("SELECT * FROM players WHERE player_id = ?", (canonical_player_id,)).fetchone()
                if row:
                    matched[row["player_id"]] = row
            if username or display_name:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM players
                    WHERE (? <> '' AND lower(username) = lower(?))
                       OR (? <> '' AND lower(display_name) = lower(?))
                    """,
                    (username, username, display_name, display_name),
                ).fetchall()
                if display_name and not has_strong_player_identifier and not allow_multiple and len(rows) > 1:
                    raise HTTPException(
                        status_code=409,
                        detail="Display name matched multiple players. Provide a stronger identifier or allow_multiple=true.",
                    )
                for row in rows:
                    matched[row["player_id"]] = row
            if recnet_id:
                try:
                    recnet_id_int = int(recnet_id)
                except ValueError:
                    recnet_id_int = 0
                if recnet_id_int > 0:
                    if api_version:
                        rows = conn.execute(
                            """
                            SELECT p.*
                            FROM players p
                            JOIN player_version_state pvs ON p.player_id = pvs.player_id
                            WHERE pvs.api_version = ?
                              AND json_extract(pvs.state_json, '$.recnet_id') = ?
                            """,
                            (api_version, recnet_id_int),
                        ).fetchall()
                    else:
                        rows = conn.execute(
                            """
                            SELECT p.*
                            FROM players p
                            JOIN player_version_state pvs ON p.player_id = pvs.player_id
                            WHERE json_extract(pvs.state_json, '$.recnet_id') = ?
                            """,
                            (recnet_id_int,),
                        ).fetchall()
                    for row in rows:
                        matched[row["player_id"]] = row
            if platform_id:
                if api_version:
                    rows = conn.execute(
                        """
                        SELECT p.*
                        FROM players p
                        JOIN player_version_state pvs ON p.player_id = pvs.player_id
                        WHERE pvs.api_version = ?
                          AND json_extract(pvs.state_json, '$.platform_id') = ?
                        """,
                        (api_version, platform_id),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT p.*
                        FROM players p
                        JOIN player_version_state pvs ON p.player_id = pvs.player_id
                        WHERE json_extract(pvs.state_json, '$.platform_id') = ?
                        """,
                        (platform_id,),
                    ).fetchall()
                for row in rows:
                    matched[row["player_id"]] = row

        if any(bool(row["is_coach"]) for row in matched.values()):
            raise HTTPException(status_code=403, detail="Coach cannot be banned.")

        banned_players: list[dict[str, Any]] = []
        room_enforcements: list[dict[str, Any]] = []
        for player in matched.values():
            context.create_player_ban(
                player["player_id"],
                reason=reason,
                extra_identities=identities,
                created_by=admin_actor_id(request),
            )
            moderation_service.append_operator_action(
                db,
                case_id=None,
                target_type="player",
                target_id=str(player["player_id"]),
                actor_id=admin_actor_id(request),
                action="ban",
                previous_state="normal",
                new_state="banned",
                reason=reason,
                metadata={"source_endpoint": "/admin/ban"},
            )
            banned_players.append(
                {
                    "player_id": player["player_id"],
                    "username": player["username"],
                    "display_name": player["display_name"],
                }
            )
            room_enforcements.append(
                {
                    "player_id": player["player_id"],
                    "result": await enforce_player_room_lock(
                        context,
                        player_id=str(player["player_id"]),
                        sanction_type="ban",
                        reason=reason,
                        duration_seconds=None,
                    ),
                }
            )

        identity_bans_added = context.create_identity_ban(identities, reason=reason)
        if not banned_players and identity_bans_added <= 0:
            raise HTTPException(status_code=409, detail="No new player or identity ban was created.")

        return JSONResponse(
            {
                "Success": True,
                "Message": "Ban applied.",
                "BannedPlayers": banned_players,
                "RoomEnforcements": room_enforcements,
                "IdentityBansAdded": identity_bans_added,
            }
        )

    @app.post("/admin/unban")
    async def admin_unban(request: Request) -> JSONResponse:
        await require_operator_request(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Admin unban payload must be JSON.") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Admin unban payload must be a JSON object.")

        api_version = admin_api_version_from_payload(payload)

        username = str(payload.get("username") or payload.get("Username") or "").strip()
        display_name = str(
            payload.get("display_name") or payload.get("displayName") or payload.get("DisplayName") or ""
        ).strip()
        canonical_player_id = str(
            payload.get("canonical_player_id")
            or payload.get("canonicalPlayerId")
            or payload.get("player_uuid")
            or ""
        ).strip()
        player_id_value = str(payload.get("player_id") or payload.get("playerId") or payload.get("PlayerId") or "").strip()
        if not canonical_player_id and re.fullmatch(r"[0-9a-fA-F-]{32,36}", player_id_value):
            canonical_player_id = player_id_value
            player_id_value = ""
        recnet_id = str(payload.get("recnet_id") or payload.get("recNetId") or payload.get("recnetId") or "").strip()
        if not recnet_id and player_id_value:
            recnet_id = player_id_value
        platform = str(payload.get("platform") or payload.get("Platform") or "").strip()
        platform_id = str(payload.get("platform_id") or payload.get("platformId") or payload.get("PlatformId") or "").strip()
        account_id = str(payload.get("account_id") or payload.get("accountId") or "").strip()
        ip = str(payload.get("ip") or payload.get("ipAddress") or payload.get("ip_address") or "").strip()
        hardware_id = str(
            payload.get("hardware_id") or payload.get("hardwareId") or payload.get("device_id") or payload.get("deviceId") or ""
        ).strip()
        allow_multiple = payload_truthy(payload, "allow_multiple", "allowMultiple", "AllowMultiple")
        has_strong_player_identifier = bool(
            username or canonical_player_id or recnet_id or platform_id or account_id or ip or hardware_id
        )

        identities: list[tuple[str, Any]] = []
        if username:
            identities.append(("username_lower", username))
        if display_name:
            identities.append(("username_lower", display_name))
        if canonical_player_id:
            identities.append(("account_id", canonical_player_id))
        if account_id:
            identities.append(("account_id", account_id))
        if recnet_id:
            append_recnet_identity_pairs(identities, recnet_id, api_version)
        if platform_id:
            identities.append(("account_id", f"platform:{platform or 0}:{platform_id}"))
        if ip:
            identities.append(("ip_hash", ip))
        if hardware_id:
            identities.append(("hardware_id_hash", hardware_id))

        if not identities and not canonical_player_id:
            raise HTTPException(status_code=400, detail="Provide at least one player or identity field to unban.")

        matched: dict[str, sqlite3.Row] = {}
        with db.connection() as conn:
            if canonical_player_id:
                row = conn.execute("SELECT * FROM players WHERE player_id = ?", (canonical_player_id,)).fetchone()
                if row:
                    matched[row["player_id"]] = row
            if username or display_name:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM players
                    WHERE (? <> '' AND lower(username) = lower(?))
                       OR (? <> '' AND lower(display_name) = lower(?))
                    """,
                    (username, username, display_name, display_name),
                ).fetchall()
                if display_name and not has_strong_player_identifier and not allow_multiple and len(rows) > 1:
                    raise HTTPException(
                        status_code=409,
                        detail="Display name matched multiple players. Provide a stronger identifier or allow_multiple=true.",
                    )
                for row in rows:
                    matched[row["player_id"]] = row
            if recnet_id:
                try:
                    recnet_id_int = int(recnet_id)
                except ValueError:
                    recnet_id_int = 0
                if recnet_id_int > 0:
                    if api_version:
                        rows = conn.execute(
                            """
                            SELECT p.*
                            FROM players p
                            JOIN player_version_state pvs ON p.player_id = pvs.player_id
                            WHERE pvs.api_version = ?
                              AND json_extract(pvs.state_json, '$.recnet_id') = ?
                            """,
                            (api_version, recnet_id_int),
                        ).fetchall()
                    else:
                        rows = conn.execute(
                            """
                            SELECT p.*
                            FROM players p
                            JOIN player_version_state pvs ON p.player_id = pvs.player_id
                            WHERE json_extract(pvs.state_json, '$.recnet_id') = ?
                            """,
                            (recnet_id_int,),
                        ).fetchall()
                    for row in rows:
                        matched[row["player_id"]] = row
            if platform_id:
                if api_version:
                    rows = conn.execute(
                        """
                        SELECT p.*
                        FROM players p
                        JOIN player_version_state pvs ON p.player_id = pvs.player_id
                        WHERE pvs.api_version = ?
                          AND json_extract(pvs.state_json, '$.platform_id') = ?
                        """,
                        (api_version, platform_id),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT p.*
                        FROM players p
                        JOIN player_version_state pvs ON p.player_id = pvs.player_id
                        WHERE json_extract(pvs.state_json, '$.platform_id') = ?
                        """,
                        (platform_id,),
                    ).fetchall()
                for row in rows:
                    matched[row["player_id"]] = row

        unbanned_players: list[dict[str, Any]] = []
        for player in matched.values():
            context.unban_player(player["player_id"])
            moderation_service.append_operator_action(
                db,
                case_id=None,
                target_type="player",
                target_id=str(player["player_id"]),
                actor_id=admin_actor_id(request),
                action="unban",
                previous_state="banned",
                new_state="normal",
                reason="Reversed by server operator.",
                metadata={"source_endpoint": "/admin/unban"},
            )
            unbanned_players.append(
                {
                    "player_id": player["player_id"],
                    "username": player["username"],
                    "display_name": player["display_name"],
                }
            )

        identity_bans_deactivated = context.unban_identities(identities)
        return JSONResponse(
            {
                "Success": True,
                "Message": "Unban applied.",
                "UnbannedPlayers": unbanned_players,
                "IdentityBansDeactivated": identity_bans_deactivated,
            }
        )

    @app.api_route("/images/{filename:path}", methods=["GET", "HEAD"], include_in_schema=False)
    @app.api_route("/IMAGES/{filename:path}", methods=["GET", "HEAD"], include_in_schema=False)
    async def serve_static_image(filename: str) -> Response:
        resp = context.serve_image(filename)
        if resp is not None:
            return resp
        raise HTTPException(status_code=404, detail="Image not found.")

    @app.api_route(
        "/{api_version}/{route_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
        include_in_schema=False,
    )
    async def dispatch_http(api_version: str, route_path: str, request: Request) -> Response:
        if api_version.casefold() in {"images", "image"}:
            resp = context.serve_image(route_path)
            if resp is not None:
                return resp
            raise HTTPException(status_code=404, detail="Image not found.")

        # April-era clients fetch transient room/holotar and invention blobs
        # from host-root CDN paths (`/data/<blob>` and `/invention/<blob>`), so
        # the first URL segment is not an API version. The unguessable blob
        # name begins with its owning adapter version; route it back to that
        # adapter without making another build guess at the payload format.
        cdn_kind = api_version.casefold()
        if cdn_kind in {"data", "invention"}:
            version_prefix = route_path.split("-", 1)[0]
            if API_VERSION_RE.fullmatch(version_prefix):
                api_version = version_prefix
                route_path = f"{cdn_kind}/{route_path}"

        resolved_api_version = resolve_api_version(api_version)
        module = None
        try:
            module = load_version_module(settings, resolved_api_version)
            await context.resolve_request_player_session(
                request, resolved_api_version
            )
            allow_sanctioned = getattr(module, "allow_sanctioned_http_route", None)
            sanction_exception = bool(
                callable(allow_sanctioned)
                and allow_sanctioned(route_path=route_path, method=request.method)
            )
            if not sanction_exception:
                context.assert_request_not_banned(request, resolved_api_version)
            handler = getattr(module, "handle_http", None)
            if handler is None:
                raise HTTPException(status_code=501, detail="HTTP API is not implemented for this version.")
            result = handler(request=request, route_path=route_path, context=context)
            awaited = maybe_await(result)
            if awaited is not None:
                result = await awaited
            if isinstance(result, Response):
                if result.status_code == 404:
                    image_filename = route_path.split("/")[-1]
                    if Path(image_filename).suffix.lower() in ALLOWED_IMAGE_EXTENSIONS:
                        image_resp = context.serve_image(image_filename)
                        if image_resp is not None:
                            return image_resp
                if should_alert_endpoint_status(result.status_code):
                    await record_and_notify_endpoint_error(
                        context=context,
                        settings=settings,
                        request=request,
                        route_path=route_path,
                        resolved_api_version=resolved_api_version,
                        module=module,
                        status_code=result.status_code,
                        error_detail="Adapter returned an error response.",
                    )
                return result
            if result is None:
                image_filename = route_path.split("/")[-1]
                if Path(image_filename).suffix.lower() in ALLOWED_IMAGE_EXTENSIONS:
                    image_resp = context.serve_image(image_filename)
                    if image_resp is not None:
                        return image_resp
                raise HTTPException(status_code=404, detail="Unknown endpoint.")
            return JSONResponse(content=result)
        except HTTPException as exc:
            if exc.status_code == 404:
                image_filename = route_path.split("/")[-1]
                if Path(image_filename).suffix.lower() in ALLOWED_IMAGE_EXTENSIONS:
                    image_resp = context.serve_image(image_filename)
                    if image_resp is not None:
                        return image_resp
            if should_alert_endpoint_status(exc.status_code):
                await record_and_notify_endpoint_error(
                    context=context,
                    settings=settings,
                    request=request,
                    route_path=route_path,
                    resolved_api_version=resolved_api_version,
                    module=module,
                    status_code=exc.status_code,
                    error_detail=exc.detail,
                )
            raise
        except Exception as exc:
            await record_and_notify_endpoint_error(
                context=context,
                settings=settings,
                request=request,
                route_path=route_path,
                resolved_api_version=resolved_api_version,
                module=module,
                status_code=500,
                error_detail=f"Internal server error ({type(exc).__name__}).",
            )
            raise

    @app.websocket("/{api_version}/{route_path:path}")
    async def dispatch_websocket(api_version: str, route_path: str, websocket: WebSocket) -> None:
        client_host = websocket.client.host if websocket.client else "unknown"
        try:
            allowed = await limiter.allow(f"ws:{client_host}")
        except HTTPException as exc:
            raise WebSocketException(
                code=status.WS_1013_TRY_AGAIN_LATER,
                reason=str(exc.detail),
            ) from exc
        if not allowed:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Rate limit exceeded.")
        try:
            resolved_api_version = resolve_api_version(api_version)
            module = load_version_module(settings, resolved_api_version)
            await context.resolve_request_player_session(
                websocket, resolved_api_version
            )
            context.assert_request_not_banned(websocket, resolved_api_version)
        except HTTPException as exc:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason=str(exc.detail)) from exc
        handler = getattr(module, "handle_websocket", None)
        if handler is None:
            raise WebSocketException(
                code=status.WS_1008_POLICY_VIOLATION,
                reason="WebSocket endpoint is not implemented for this version.",
            )
        result = handler(websocket=websocket, route_path=route_path, context=context)
        awaited = maybe_await(result)
        if awaited is not None:
            await awaited

    @app.on_event("startup")
    async def restore_persisted_maintenance_after_restart() -> None:
        try:
            await transient.start()
        except redis_state.RedisConfigurationError as exc:
            raise RuntimeError(str(exc)) from exc
        with db.connection() as conn:
            banned_player_ids = [
                str(row["player_id"])
                for row in conn.execute("SELECT player_id FROM players WHERE is_banned = 1")
            ]
        for banned_player_id in banned_player_ids:
            await transient.revoke_player_transient_state(
                banned_player_id,
                aliases=context.transient_player_aliases(banned_player_id),
            )
        # The absolute UTC schedule is canonical. Restarting or waking the
        # process must preserve the original deadline rather than cancelling or
        # restarting its countdown.
        state = context.get_maintenance_state()
        if bool(state.get("active")):
            await arm_maintenance_deadline(context, state)

    @app.on_event("shutdown")
    async def stop_persisted_maintenance_deadline() -> None:
        await stop_maintenance_deadlines(context)
        await transient.close()

    app.state.settings = settings
    app.state.context = context
    app.state.transient = transient
    return app


app = create_app()


def _environment_flag_enabled(name: str) -> bool:
    return str(os.getenv(name) or "").strip().casefold() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _read_uvicorn_tls_files(settings: Settings | None = None) -> tuple[str, str] | None:
    certfile = os.getenv(TLS_CERTFILE_ENV_NAME)
    keyfile = os.getenv(TLS_KEYFILE_ENV_NAME)
    if not certfile and not keyfile:
        effective_settings = settings or app.state.settings
        if (
            effective_settings.is_railway
            or _environment_flag_enabled(DISABLE_LOCAL_TLS_ENV_NAME)
        ):
            return None

        default_certfile = (
            effective_settings.root_dir / "TLS" / DEFAULT_LOCAL_TLS_CERTFILE
        )
        default_keyfile = (
            effective_settings.root_dir / "TLS" / DEFAULT_LOCAL_TLS_KEYFILE
        )
        if not default_certfile.exists() and not default_keyfile.exists():
            return None
        certfile = str(default_certfile)
        keyfile = str(default_keyfile)
    if not certfile or not keyfile:
        raise ConfigurationError(
            f"{TLS_CERTFILE_ENV_NAME} and {TLS_KEYFILE_ENV_NAME} must be set together."
        )

    resolved_certfile = Path(certfile).expanduser().resolve()
    resolved_keyfile = Path(keyfile).expanduser().resolve()
    if not resolved_certfile.is_file():
        raise ConfigurationError(f"TLS certificate file does not exist: {resolved_certfile}")
    if not resolved_keyfile.is_file():
        raise ConfigurationError(f"TLS private-key file does not exist: {resolved_keyfile}")
    return str(resolved_certfile), str(resolved_keyfile)


def _create_loopback_backend_socket() -> tuple[socket.socket, int]:
    backend_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    backend_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    backend_socket.bind(("127.0.0.1", 0))
    backend_socket.listen(2048)
    return backend_socket, int(backend_socket.getsockname()[1])


async def _relay_stream(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    try:
        while data := await reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass


async def _proxy_shared_port_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    http_backend_port: int,
    https_backend_port: int,
) -> None:
    backend_writer: asyncio.StreamWriter | None = None
    relay_tasks: set[asyncio.Task[None]] = set()
    try:
        first_byte = await asyncio.wait_for(client_reader.read(1), timeout=15)
        if not first_byte:
            return
        is_tls = first_byte[0] in {0x16, 0x80}
        backend_port = https_backend_port if is_tls else http_backend_port
        backend_reader, backend_writer = await asyncio.open_connection(
            "127.0.0.1",
            backend_port,
        )
        backend_writer.write(first_byte)
        await backend_writer.drain()

        relay_tasks = {
            asyncio.create_task(_relay_stream(client_reader, backend_writer)),
            asyncio.create_task(_relay_stream(backend_reader, client_writer)),
        }
        _, pending = await asyncio.wait(
            relay_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*relay_tasks, return_exceptions=True)
    except (ConnectionError, asyncio.TimeoutError):
        pass
    finally:
        for task in relay_tasks:
            if not task.done():
                task.cancel()
        if backend_writer is not None:
            backend_writer.close()
            with suppress(Exception):
                await backend_writer.wait_closed()
        client_writer.close()
        with suppress(Exception):
            await client_writer.wait_closed()


async def _serve_shared_http_https_port(
    *,
    host: str,
    port: int,
    http_backend_port: int,
    https_backend_port: int,
) -> None:
    server = await asyncio.start_server(
        lambda reader, writer: _proxy_shared_port_connection(
            reader,
            writer,
            http_backend_port=http_backend_port,
            https_backend_port=https_backend_port,
        ),
        host=host,
        port=port,
        backlog=2048,
    )
    print(
        "Shared local listener ready: "
        f"http://localhost:{port} and https://localhost:{port}",
        flush=True,
    )
    async with server:
        await server.serve_forever()


def _wait_for_backend_start(
    *,
    server: Any,
    thread: threading.Thread,
    errors: list[BaseException],
    label: str,
) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if server.started:
            return
        if errors:
            raise RuntimeError(f"{label} backend failed to start.") from errors[0]
        if not thread.is_alive():
            raise RuntimeError(f"{label} backend stopped during startup.")
        time.sleep(0.05)
    raise RuntimeError(f"{label} backend did not start within 15 seconds.")


def _run_shared_http_https_server(
    uvicorn_module: Any,
    *,
    tls_files: tuple[str, str],
) -> None:
    http_socket, http_backend_port = _create_loopback_backend_socket()
    https_socket, https_backend_port = _create_loopback_backend_socket()
    http_server = uvicorn_module.Server(
        uvicorn_module.Config(
            app,
            host="127.0.0.1",
            port=http_backend_port,
            lifespan="on",
        )
    )
    https_server = uvicorn_module.Server(
        uvicorn_module.Config(
            app,
            host="127.0.0.1",
            port=https_backend_port,
            lifespan="off",
            ssl_certfile=tls_files[0],
            ssl_keyfile=tls_files[1],
        )
    )
    backend_errors: list[BaseException] = []

    def run_backend(server: Any, backend_socket: socket.socket) -> None:
        try:
            server.run(sockets=[backend_socket])
        except BaseException as exc:
            backend_errors.append(exc)

    http_thread = threading.Thread(
        target=run_backend,
        args=(http_server, http_socket),
        name="recroom-http-backend",
        daemon=True,
    )
    https_thread = threading.Thread(
        target=run_backend,
        args=(https_server, https_socket),
        name="recroom-https-backend",
        daemon=True,
    )

    try:
        http_thread.start()
        _wait_for_backend_start(
            server=http_server,
            thread=http_thread,
            errors=backend_errors,
            label="HTTP",
        )
        https_thread.start()
        _wait_for_backend_start(
            server=https_server,
            thread=https_thread,
            errors=backend_errors,
            label="HTTPS",
        )
        asyncio.run(
            _serve_shared_http_https_port(
                host=app.state.settings.host,
                port=app.state.settings.port,
                http_backend_port=http_backend_port,
                https_backend_port=https_backend_port,
            )
        )
    except KeyboardInterrupt:
        pass
    finally:
        http_server.should_exit = True
        https_server.should_exit = True
        if http_thread.is_alive():
            http_thread.join(timeout=15)
        if https_thread.is_alive():
            https_thread.join(timeout=15)
        with suppress(OSError):
            http_socket.close()
        with suppress(OSError):
            https_socket.close()


if __name__ == "__main__":
    import uvicorn

    tls_files = _read_uvicorn_tls_files(app.state.settings)
    if tls_files is not None and not app.state.settings.is_railway:
        _run_shared_http_https_server(uvicorn, tls_files=tls_files)
    else:
        uvicorn_options: dict[str, Any] = {
            "host": app.state.settings.host,
            "port": app.state.settings.port,
        }
        if tls_files is not None:
            uvicorn_options["ssl_certfile"], uvicorn_options["ssl_keyfile"] = tls_files
        uvicorn.run(app, **uvicorn_options)
