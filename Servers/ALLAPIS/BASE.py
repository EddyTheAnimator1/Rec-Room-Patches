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
import time
import uuid
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketException
from fastapi.responses import JSONResponse, Response
from starlette import status


API_VERSION_RE = re.compile(r"^[A-Za-z0-9_]+$")
IMAGE_DATA_DIR_NAME = "IMAGES"
ALLOWED_DATA_ROOT_EXTENSIONS = {".json"}
ALLOWED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg"}
SQLITE_SIDECAR_RE = re.compile(r"^database\.sqlite3(?:-(?:journal|wal|shm))?$")
DEFAULT_LOCAL_PORT = 7979
DEFAULT_CREATED_PLAYER_EMAIL = "idontwanttoguess@gmail.com"
DEV_PERMISSIONS = ["DEV"]
COACH_PLAYER_ID = "00000000-0000-0000-0000-000000000099"
ADMIN_KEY_ENV_NAMES = ("RECROOM_ADMIN_KEY", "RR_ADMIN_KEY")


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
        return resolved.suffix.lower() in ALLOWED_DATA_ROOT_EXTENSIONS
    if len(relative.parts) == 2 and relative.parts[0] == IMAGE_DATA_DIR_NAME:
        return resolved.suffix.lower() in ALLOWED_IMAGE_EXTENSIONS
    return False


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
    ensure_coach_profile(db)
    cleanup_existing_banned_players(db)


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
    value = str(request.headers.get("x-rec-room-admin-key") or "").strip()
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

    def get_motd(self, api_version: str) -> str:
        env_key = f"RR_MOTD_{api_version.upper()}"
        for key in (env_key, "RECROOM_MOTD"):
            value = os.getenv(key)
            if value is not None:
                return value

        setting_keys = (f"{api_version}.motd", "motd")
        with self.db.connection() as conn:
            for setting_key in setting_keys:
                row = conn.execute("SELECT value_json FROM server_settings WHERE key = ?", (setting_key,)).fetchone()
                value = self._decode_setting_string(row)
                if value is not None:
                    return value

            row = conn.execute(
                """
                SELECT value_json
                FROM server_settings
                WHERE key LIKE '%.motd'
                ORDER BY created_at ASC
                LIMIT 1
                """
            ).fetchone()
            value = self._decode_setting_string(row)
            return value if value is not None else ""

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
            return raw_value
        return value if isinstance(value, str) else None

    def set_shared_motd(self, message: str) -> None:
        self._set_server_setting("motd", message)

    def set_motd(self, api_version: str, message: str) -> None:
        self._set_server_setting(f"{api_version}.motd", message)

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
        if not recnet_id:
            return None
        try:
            recnet_id_value = int(recnet_id)
        except ValueError:
            return None
        with self.db.connection() as conn:
            return conn.execute(
                """
                SELECT p.*, pvs.state_json
                FROM players p
                JOIN player_version_state pvs ON p.player_id = pvs.player_id
                WHERE pvs.api_version = ?
                  AND json_extract(pvs.state_json, '$.recnet_id') = ?
                """,
                (api_version, recnet_id_value),
            ).fetchone()

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

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        events = self._events[key]
        cutoff = now - self.window_seconds
        while events and events[0] < cutoff:
            events.popleft()
        if len(events) >= self.limit:
            return False
        events.append(now)
        return True


def maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value):
        return value
    return None


def load_version_module(settings: Settings, api_version: str) -> Any:
    if not API_VERSION_RE.fullmatch(api_version):
        raise HTTPException(status_code=404, detail="Unknown API version.")
    module_path = settings.api_dir / f"{api_version}.py"
    if not module_path.is_file():
        raise HTTPException(status_code=404, detail="Unknown API version.")
    module_name = f"recroom_api_{api_version}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise HTTPException(status_code=500, detail="API module could not be loaded.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
        client_host = request.client.host if request.client else "unknown"
        if not limiter.allow(f"http:{client_host}"):
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded."})
        return await call_next(request)

    @app.post("/admin/ban")
    async def admin_ban(request: Request) -> JSONResponse:
        require_admin_key(request)
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Admin ban payload must be JSON.") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Admin ban payload must be a JSON object.")

        api_version = str(payload.get("api_version") or payload.get("apiVersion") or "12january2018").strip()
        if not API_VERSION_RE.fullmatch(api_version):
            raise HTTPException(status_code=400, detail="Invalid api_version.")
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
            identities.append(("account_id", f"{api_version}:recnet:{recnet_id}"))
            identities.append(("account_id", f"local-{api_version}-{recnet_id}"))
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
                for row in rows:
                    matched[row["player_id"]] = row
            if recnet_id:
                try:
                    recnet_id_int = int(recnet_id)
                except ValueError:
                    recnet_id_int = 0
                if recnet_id_int > 0:
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
                    for row in rows:
                        matched[row["player_id"]] = row
            if platform_id:
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

    @app.api_route(
        "/{api_version}/{route_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    )
    async def dispatch_http(api_version: str, route_path: str, request: Request) -> Response:
        module = load_version_module(settings, api_version)
        context.assert_request_not_banned(request, api_version)
        handler = getattr(module, "handle_http", None)
        if handler is None:
            raise HTTPException(status_code=501, detail="HTTP API is not implemented for this version.")
        result = handler(request=request, route_path=route_path, context=context)
        awaited = maybe_await(result)
        if awaited is not None:
            result = await awaited
        if isinstance(result, Response):
            return result
        if result is None:
            raise HTTPException(status_code=404, detail="Unknown endpoint.")
        return JSONResponse(content=result)

    @app.websocket("/{api_version}/{route_path:path}")
    async def dispatch_websocket(api_version: str, route_path: str, websocket: WebSocket) -> None:
        client_host = websocket.client.host if websocket.client else "unknown"
        if not limiter.allow(f"ws:{client_host}"):
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Rate limit exceeded.")
        try:
            module = load_version_module(settings, api_version)
            context.assert_request_not_banned(websocket, api_version)
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
