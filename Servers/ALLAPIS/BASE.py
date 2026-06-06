from __future__ import annotations

import asyncio
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
    with db.connect() as conn:
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
        with self.db.connect() as conn:
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
        return get_or_create_player(self.db, api_version=api_version, **kwargs)

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

    @app.api_route(
        "/{api_version}/{route_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    )
    async def dispatch_http(api_version: str, route_path: str, request: Request) -> Response:
        module = load_version_module(settings, api_version)
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
