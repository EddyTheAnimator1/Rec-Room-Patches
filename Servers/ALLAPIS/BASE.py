from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import mimetypes
import os
import re
import secrets
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable, Iterator

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketException
from fastapi.responses import JSONResponse, Response
from starlette import status


API_VERSION_RE = re.compile(r"^[A-Za-z0-9_]+$")
API_VERSION_ALIASES: dict[str, str] = {}
IMAGE_DATA_DIR_NAME = "IMAGES"
ALLOWED_DATA_ROOT_EXTENSIONS = {".json"}
ALLOWED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg"}
SQLITE_SIDECAR_RE = re.compile(r"^database\.sqlite3(?:-(?:journal|wal|shm))?$")
ROBOTS_TXT_FILENAME = "robots.txt"
DEFAULT_ROBOTS_TXT = "User-agent: OAI-SearchBot\nDisallow: /\n\nUser-agent: GPTBot\nDisallow: /\n"
DEFAULT_LOCAL_PORT = 7979
DEFAULT_RAILWAY_PUBLIC_BASE_URL = "https://brand-new-all-production.up.railway.app"
PUBLIC_BASE_URL_ENV_NAMES = (
    "RECROOM_PUBLIC_BASE_URL",
    "RECROOM_API_PUBLIC_BASE_URL",
    "RECROOM_SERVER_PUBLIC_BASE_URL",
)
DEFAULT_MAX_REQUEST_BODY_BYTES = 8 * 1024 * 1024
DEFAULT_CREATED_PLAYER_EMAIL = "idontwanttoguess@gmail.com"
DEV_PERMISSIONS = ["DEV"]
COACH_PLAYER_ID = "00000000-0000-0000-0000-000000000099"
ADMIN_KEY_ENV_NAMES = ("RECROOM_ADMIN_BAN_KEY", "RECROOM_ADMIN_SECRET", "RECROOM_ADMIN_KEY", "RR_ADMIN_KEY")
ERROR_WEBHOOK_ENV_NAMES = ("RECROOM_ERROR_WEBHOOK_URL", "RECROOM_API_ERROR_WEBHOOK_URL", "DISCORD_ERROR_WEBHOOK_URL")
DISCORD_RED_COLOR = 0xFF0000
WEBHOOK_ALERT_TITLE = "⋆｡°✩ Endpoint ghost detected ✩°｡⋆"
WEBHOOK_ALERT_MESSAGE = "Someone was waiting for a ghost. ."
MAX_WEBHOOK_FIELD_VALUE_LENGTH = 900
SENSITIVE_PAYLOAD_KEY_RE = re.compile(r"(?i)(token|secret|password|authorization|webhook|cookie|session|email|admin)")


class ConfigurationError(RuntimeError):
    pass


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


def public_api_base_url(request: Request, context: Any, api_version: str) -> str:
    settings = getattr(context, "settings", None)
    if getattr(settings, "is_railway", False):
        origin = _configured_public_base_url() or DEFAULT_RAILWAY_PUBLIC_BASE_URL
    else:
        origin = _request_origin(request, settings)
    return f"{origin.rstrip('/')}/{api_version.strip('/')}/"


def load_settings() -> Settings:
    root_dir = Path(__file__).resolve().parent
    is_railway = _is_railway_environment()
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
        host=os.getenv("HOST", "0.0.0.0"),
        ban_hash_pepper=ban_hash_pepper,
        max_request_body_bytes=_read_max_request_body_bytes(),
        error_webhook_url=_read_error_webhook_url(),
    )


def ensure_runtime_directories(settings: Settings) -> dict[str, str]:
    settings.api_dir.mkdir(parents=True, exist_ok=True)
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    (settings.data_dir / IMAGE_DATA_DIR_NAME).mkdir(parents=True, exist_ok=True)
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
        if name == ROBOTS_TXT_FILENAME:
            return True
        return resolved.suffix.lower() in ALLOWED_DATA_ROOT_EXTENSIONS
    if len(relative.parts) == 2 and relative.parts[0] == IMAGE_DATA_DIR_NAME:
        return resolved.suffix.lower() in ALLOWED_IMAGE_EXTENSIONS
    return False


def robots_txt_candidate_paths(settings: Settings) -> list[Path]:
    candidates = [
        settings.data_dir / ROBOTS_TXT_FILENAME,
        settings.root_dir / ROBOTS_TXT_FILENAME,
    ]

    data_parent = settings.data_dir.parent
    if data_parent != settings.data_dir:
        candidates.insert(1, data_parent / ROBOTS_TXT_FILENAME)

    unique_candidates: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            resolved = candidate
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_candidates.append(candidate)
    return unique_candidates


def read_robots_txt(settings: Settings) -> str:
    for candidate in robots_txt_candidate_paths(settings):
        if candidate.is_file():
            return candidate.read_text(encoding="utf-8")
    return DEFAULT_ROBOTS_TXT


def enforce_data_directory_policy(data_dir: Path) -> None:
    for child in data_dir.iterdir():
        if child.is_dir():
            if child.name != IMAGE_DATA_DIR_NAME:
                raise ConfigurationError(f"DATA may not contain subdirectories except {IMAGE_DATA_DIR_NAME}: {child.name}")
            for nested in child.rglob("*"):
                if nested.is_dir():
                    raise ConfigurationError(f"DATA/{IMAGE_DATA_DIR_NAME} may not contain subdirectories: {nested.name}")
                if nested.is_file() and not is_allowed_data_file(nested, data_dir):
                    raise ConfigurationError(f"Unsupported file in DATA/{IMAGE_DATA_DIR_NAME}: {nested.name}")
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


def validate_image_write_path(data_dir: Path, filename: str) -> Path:
    if Path(filename).name != filename:
        raise ValueError("Image filename must not include path separators.")
    if Path(filename).suffix.lower() not in ALLOWED_IMAGE_EXTENSIONS:
        raise ValueError("DATA/IMAGES only accepts .png, .jpg, and .jpeg files.")
    image_dir = (data_dir / IMAGE_DATA_DIR_NAME).resolve()
    image_dir.mkdir(parents=True, exist_ok=True)
    path = (image_dir / filename).resolve()
    if image_dir not in path.parents:
        raise ValueError("Image write path escaped DATA/IMAGES.")
    return path


def migrate_legacy_root_images(data_dir: Path) -> dict[str, str]:
    image_dir = data_dir / IMAGE_DATA_DIR_NAME
    image_dir.mkdir(parents=True, exist_ok=True)
    moved: dict[str, str] = {}
    for child in data_dir.iterdir():
        if not child.is_file() or child.suffix.lower() not in ALLOWED_IMAGE_EXTENSIONS:
            continue
        target = image_dir / child.name
        if target.exists():
            target = image_dir / f"{child.stem}-{uuid.uuid4().hex}{child.suffix.lower()}"
        child.replace(target)
        moved[child.name] = f"{IMAGE_DATA_DIR_NAME}/{target.name}"
    return moved


def migrate_legacy_data_asset_records(db: Database, legacy_image_moves: dict[str, str]) -> None:
    updates = dict(legacy_image_moves)
    with db.connection() as conn:
        rows = conn.execute("SELECT relative_path FROM data_assets").fetchall()
    for row in rows:
        relative_path = row["relative_path"]
        if "/" not in relative_path and "\\" not in relative_path and Path(relative_path).suffix.lower() in ALLOWED_IMAGE_EXTENSIONS:
            updates.setdefault(relative_path, f"{IMAGE_DATA_DIR_NAME}/{relative_path}")
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
    if not expected or len(expected) < 64:
        raise HTTPException(status_code=503, detail="Admin API key is not configured.")
    provided = admin_key_from_request(request)
    if not provided or not secrets.compare_digest(provided, expected):
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
    def __init__(self, settings: Settings, db: Database):
        self.settings = settings
        self.db = db

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
            pairs.append(("account_id", authorization))
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
        return pairs

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
        token_prefix = f"local-{api_version}-"
        if not recnet_id and authorization.casefold().startswith(token_prefix.casefold()):
            recnet_id = authorization[len(token_prefix) :].strip()
        with self.db.connection() as conn:
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
            if authorization:
                return conn.execute(
                    """
                    SELECT p.*, pvs.state_json
                    FROM players p
                    JOIN player_version_state pvs ON p.player_id = pvs.player_id
                    WHERE pvs.api_version = ?
                      AND json_extract(pvs.state_json, '$.login_token') = ?
                    """,
                    (api_version, authorization),
                ).fetchone()
        return None

    def assert_player_not_banned(self, player_id: str) -> None:
        with self.db.connection() as conn:
            row = conn.execute("SELECT * FROM players WHERE player_id = ?", (player_id,)).fetchone()
        if row and bool(row["is_banned"]):
            self.enforce_ban_cleanup(player_id)
            raise HTTPException(status_code=403, detail=row["ban_reason"] or "This account is banned.")

    def enforce_ban_cleanup(self, player_id: str) -> None:
        with self.db.connection() as conn:
            player = conn.execute(
                "SELECT username, display_name, profile_picture_asset_id, ban_reason FROM players WHERE player_id = ?",
                (player_id,),
            ).fetchone()
            asset_id = player["profile_picture_asset_id"] if player else None
            identities = conn.execute(
                "SELECT identity_type, identity_hash FROM player_identities WHERE player_id = ?",
                (player_id,),
            ).fetchall()
            assets = conn.execute(
                """
                SELECT asset_id, relative_path
                FROM data_assets
                WHERE owner_player_id = ?
                  AND (purpose LIKE '%.profile_image' OR asset_id = ?)
                """,
                (player_id, asset_id),
            ).fetchall()
        self.record_player_identities(
            player_id,
            [
                ("account_id", player_id),
                ("username_lower", player["username"] if player else ""),
                ("username_lower", player["display_name"] if player else ""),
            ],
        )
        data_dir = self.data_dir.resolve()
        for asset in assets:
            image_path = (self.data_dir / asset["relative_path"]).resolve()
            if data_dir in image_path.parents and image_path.is_file():
                image_path.unlink()
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM sessions WHERE player_id = ?", (player_id,))
            conn.execute(
                """
                DELETE FROM data_assets
                WHERE owner_player_id = ?
                  AND (purpose LIKE '%.profile_image' OR asset_id = ?)
                """,
                (player_id, asset_id),
            )
            conn.execute(
                """
                UPDATE players
                SET profile_picture_asset_id = NULL,
                    updated_at = ?
                WHERE player_id = ?
                """,
                (utc_now(), player_id),
            )
            now = utc_now()
            refreshed_identities = conn.execute(
                "SELECT identity_type, identity_hash FROM player_identities WHERE player_id = ?",
                (player_id,),
            ).fetchall()
            for identity in list(identities) + list(refreshed_identities):
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
            rows = conn.execute(
                "SELECT api_version, state_json FROM player_version_state WHERE player_id = ?",
                (player_id,),
            ).fetchall()
            for row in rows:
                try:
                    state = json.loads(row["state_json"])
                except Exception:
                    state = {}
                for key in ("profile_image_name", "ProfileImageName"):
                    state.pop(key, None)
                conn.execute(
                    """
                    UPDATE player_version_state
                    SET state_json = ?, updated_at = ?
                    WHERE player_id = ? AND api_version = ?
                    """,
                    (json.dumps(state, sort_keys=True), utc_now(), player_id, row["api_version"]),
                )

    def create_player_ban(
        self,
        player_id: str,
        *,
        reason: str | None = None,
        extra_identities: list[tuple[str, Any]] | None = None,
    ) -> None:
        now = utc_now()
        identities = list(extra_identities or [])
        with self.db.connection() as conn:
            row = conn.execute("SELECT username, display_name FROM players WHERE player_id = ?", (player_id,)).fetchone()
            existing = conn.execute(
                "SELECT identity_type, identity_hash FROM player_identities WHERE player_id = ?",
                (player_id,),
            ).fetchall()
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
            for identity_type, value in identities:
                identity_hash = self.identity_hash(identity_type, value)
                if identity_hash:
                    conn.execute(
                        """
                        INSERT INTO bans(id, player_id, identity_type, identity_hash, reason, active, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                        """,
                        (str(uuid.uuid4()), player_id, identity_type, identity_hash, reason, now, now),
                    )
            for row in existing:
                conn.execute(
                    """
                    INSERT INTO bans(id, player_id, identity_type, identity_hash, reason, active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (str(uuid.uuid4()), player_id, row["identity_type"], row["identity_hash"], reason, now, now),
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
        path = validate_image_write_path(self.data_dir, filename)
        path.write_bytes(content)
        relative_path = f"{IMAGE_DATA_DIR_NAME}/{filename}"

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


class RateLimiter:
    def __init__(self, *, limit: int, window_seconds: int):
        self.limit = limit
        self.window_seconds = window_seconds
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._next_sweep = 0.0

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        if now >= self._next_sweep:
            self._sweep(now)
        events = self._events[key]
        cutoff = now - self.window_seconds
        while events and events[0] < cutoff:
            events.popleft()
        if len(events) >= self.limit:
            return False
        events.append(now)
        return True

    def _sweep(self, now: float) -> None:
        cutoff = now - self.window_seconds
        for key in list(self._events):
            events = self._events[key]
            while events and events[0] < cutoff:
                events.popleft()
            if not events:
                del self._events[key]
        self._next_sweep = now + self.window_seconds


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


def create_app() -> FastAPI:
    settings = load_settings()
    legacy_image_moves = ensure_runtime_directories(settings)
    db = Database(settings.db_path)
    initialize_database(db)
    migrate_legacy_data_asset_records(db, legacy_image_moves)
    context = ServerContext(settings, db)
    limiter = RateLimiter(limit=120 if settings.is_railway else 600, window_seconds=60)

    app = FastAPI(
        title="Rec Room API Restoring Server",
        debug=False,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

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
        if not limiter.allow(f"http:{client_host}"):
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded."})
        return await call_next(request)

    @app.api_route("/robots.txt", methods=["GET", "HEAD"], include_in_schema=False)
    async def robots_txt() -> Response:
        try:
            body = read_robots_txt(settings)
        except OSError as exc:
            raise HTTPException(status_code=500, detail="robots.txt could not be read.") from exc
        return Response(content=body, media_type="text/plain; charset=utf-8")

    @app.get("/admin/motd")
    async def admin_get_motd(request: Request) -> JSONResponse:
        require_admin_key(request)
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
        require_admin_key(request)
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

    @app.get("/admin/ban/status")
    async def admin_ban_status(request: Request) -> JSONResponse:
        require_admin_key(request)
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
        require_admin_key(request)
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
        for player in matched.values():
            context.create_player_ban(player["player_id"], reason=reason, extra_identities=identities)
            banned_players.append(
                {
                    "player_id": player["player_id"],
                    "username": player["username"],
                    "display_name": player["display_name"],
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
                "IdentityBansAdded": identity_bans_added,
            }
        )

    @app.post("/admin/unban")
    async def admin_unban(request: Request) -> JSONResponse:
        require_admin_key(request)
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

    @app.api_route(
        "/{api_version}/{route_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    )
    async def dispatch_http(api_version: str, route_path: str, request: Request) -> Response:
        resolved_api_version = resolve_api_version(api_version)
        module = None
        try:
            module = load_version_module(settings, resolved_api_version)
            context.assert_request_not_banned(request, resolved_api_version)
            handler = getattr(module, "handle_http", None)
            if handler is None:
                raise HTTPException(status_code=501, detail="HTTP API is not implemented for this version.")
            result = handler(request=request, route_path=route_path, context=context)
            awaited = maybe_await(result)
            if awaited is not None:
                result = await awaited
            if isinstance(result, Response):
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
                raise HTTPException(status_code=404, detail="Unknown endpoint.")
            return JSONResponse(content=result)
        except HTTPException as exc:
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
        if not limiter.allow(f"ws:{client_host}"):
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Rate limit exceeded.")
        try:
            resolved_api_version = resolve_api_version(api_version)
            module = load_version_module(settings, resolved_api_version)
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

    app.state.settings = settings
    app.state.context = context
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=app.state.settings.host, port=app.state.settings.port)
