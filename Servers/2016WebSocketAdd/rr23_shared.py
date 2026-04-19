
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import threading
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import psycopg
from psycopg.rows import dict_row

DATA_DIR = Path(os.environ.get("DATA_DIR") or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH") or ".").resolve()
DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL") or ""
INIT_LOCK = threading.Lock()
INIT_COMPLETE = False
DB_SCHEMA_LOCK_ID = int(os.environ.get("DB_SCHEMA_LOCK_ID", "72016023001"))

DEFAULT_PLAYER_NAME = os.environ.get("DEFAULT_PLAYER_NAME", "Eduard")
DEFAULT_PLATFORM = int(os.environ.get("DEFAULT_PLATFORM", "0"))
DEFAULT_LEVEL = max(1, int(os.environ.get("DEFAULT_LEVEL", "1")))
DEFAULT_XP = max(0, int(os.environ.get("DEFAULT_XP", "0")))
DEFAULT_REPUTATION = int(os.environ.get("DEFAULT_REPUTATION", "0"))
DEFAULT_VERIFIED_EMAIL = os.environ.get("DEFAULT_VERIFIED_EMAIL", "NotAnEmail@gmail.com")
DEFAULT_MOTD_TEXT = os.environ.get("DEFAULT_MOTD_TEXT", "Online on RecNet! Welcome to Rec Room!")
AUTH_USERNAME = os.environ.get("AUTH_USERNAME", "recroom@gmail.com")
AUTH_PASSWORD = os.environ.get("AUTH_PASSWORD", "recnet87")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def datetime_to_dotnet_ticks(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    unix_us = int(value.timestamp() * 1_000_000)
    return 621355968000000000 + (unix_us * 10)


def utcnow_ticks() -> int:
    return datetime_to_dotnet_ticks(datetime.now(timezone.utc))


def parse_dotnet_ticks(value: Any, default: int | None = None) -> int:
    if default is None:
        default = utcnow_ticks()
    if isinstance(value, (int, float)):
        ivalue = int(value)
        return ivalue if ivalue > 0 else default
    raw = str(value or '').strip()
    if not raw:
        return default
    if raw.lstrip('-').isdigit():
        ivalue = int(raw)
        return ivalue if ivalue > 0 else default
    try:
        return datetime_to_dotnet_ticks(datetime.fromisoformat(raw.replace('Z', '+00:00')))
    except Exception:
        return default


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _resolve_connection_kwargs() -> dict[str, Any]:
    if DATABASE_URL:
        return {"conninfo": DATABASE_URL}

    host = os.environ.get("PGHOST", "").strip()
    database = os.environ.get("PGDATABASE", "").strip()
    user = os.environ.get("PGUSER", "").strip()
    if not (host and database and user):
        raise RuntimeError(
            "PostgreSQL is required. Set DATABASE_URL or PGHOST/PGDATABASE/PGUSER/PGPASSWORD."
        )
    return {
        "host": host,
        "port": int(os.environ.get("PGPORT", "5432") or "5432"),
        "dbname": database,
        "user": user,
        "password": os.environ.get("PGPASSWORD", ""),
        "sslmode": os.environ.get("PGSSLMODE", "prefer"),
    }


def _adapt_query(query: str) -> str:
    adapted = query
    insert_or_ignore = "INSERT OR IGNORE INTO" in adapted.upper()
    if insert_or_ignore:
        adapted = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", adapted, flags=re.IGNORECASE)
    adapted = adapted.replace("?", "%s")
    if insert_or_ignore:
        adapted = f"{adapted.rstrip()} ON CONFLICT DO NOTHING"
    return adapted


class PgCursorResult:
    def __init__(self, cursor: psycopg.Cursor[Any]) -> None:
        self._cursor = cursor

    def fetchone(self) -> dict[str, Any] | None:
        row = self._cursor.fetchone()
        return None if row is None else dict(row)

    def fetchall(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._cursor.fetchall()]

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount


class PgConnection:
    def __init__(self, raw: psycopg.Connection[Any]) -> None:
        self._raw = raw

    def execute(self, query: str, params: tuple[Any, ...] | list[Any] = ()) -> PgCursorResult:
        cursor = self._raw.cursor(row_factory=dict_row)
        cursor.execute(_adapt_query(query), params)
        return PgCursorResult(cursor)

    def executescript(self, script: str) -> None:
        for statement in (chunk.strip() for chunk in script.split(';')):
            if statement:
                self.execute(statement)

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        self._raw.close()


def connect() -> PgConnection:
    ensure_data_dir()
    raw = psycopg.connect(**_resolve_connection_kwargs())
    return PgConnection(raw)


def init_db(force: bool = False) -> None:
    global INIT_COMPLETE
    if INIT_COMPLETE and not force:
        return

    with INIT_LOCK:
        if INIT_COMPLETE and not force:
            return

        with closing(connect()) as conn:
            advisory_lock_acquired = False
            try:
                conn.execute("SELECT pg_advisory_lock(?)", (DB_SCHEMA_LOCK_ID,))
                advisory_lock_acquired = True
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS players (
                        id BIGINT PRIMARY KEY,
                        platform INTEGER NOT NULL,
                        platform_id BIGINT NOT NULL,
                        name TEXT NOT NULL,
                        display_name TEXT NOT NULL,
                        username TEXT NOT NULL,
                        xp INTEGER NOT NULL,
                        level INTEGER NOT NULL,
                        reputation INTEGER NOT NULL,
                        email TEXT NOT NULL,
                        verified INTEGER NOT NULL DEFAULT 1,
                        developer INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_players_platform_platformid
                        ON players(platform, platform_id);

                    CREATE TABLE IF NOT EXISTS settings (
                        player_id BIGINT NOT NULL,
                        key TEXT NOT NULL,
                        value TEXT NOT NULL,
                        PRIMARY KEY (player_id, key)
                    );

                    CREATE TABLE IF NOT EXISTS avatars (
                        player_id BIGINT PRIMARY KEY,
                        outfit_selections TEXT NOT NULL DEFAULT '',
                        skin_color TEXT NOT NULL DEFAULT '',
                        hair_color TEXT NOT NULL DEFAULT ''
                    );

                    CREATE TABLE IF NOT EXISTS avatar_items (
                        player_id BIGINT NOT NULL,
                        avatar_item_desc TEXT NOT NULL,
                        unlocked_level INTEGER NOT NULL DEFAULT 1,
                        PRIMARY KEY (player_id, avatar_item_desc)
                    );

                    CREATE TABLE IF NOT EXISTS presence (
                        player_id BIGINT PRIMARY KEY,
                        game_session_id TEXT NOT NULL DEFAULT '',
                        app_version TEXT NOT NULL DEFAULT '',
                        last_update_time TEXT NOT NULL,
                        activity TEXT NOT NULL DEFAULT 'DormRoom',
                        private INTEGER NOT NULL DEFAULT 0,
                        available_space INTEGER NOT NULL DEFAULT 0,
                        game_in_progress INTEGER NOT NULL DEFAULT 0
                    );

                    CREATE TABLE IF NOT EXISTS relationships (
                        player_id BIGINT NOT NULL,
                        other_player_id BIGINT NOT NULL,
                        relationship_type INTEGER NOT NULL,
                        PRIMARY KEY (player_id, other_player_id)
                    );

                    CREATE TABLE IF NOT EXISTS messages (
                        id BIGSERIAL PRIMARY KEY,
                        from_player_id BIGINT NOT NULL,
                        to_player_id BIGINT NOT NULL,
                        sent_time TEXT NOT NULL,
                        type INTEGER NOT NULL,
                        data TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS game_sessions (
                        id TEXT PRIMARY KEY,
                        app_version TEXT NOT NULL DEFAULT '',
                        activity TEXT NOT NULL DEFAULT 'DormRoom',
                        private INTEGER NOT NULL DEFAULT 0,
                        available_space INTEGER NOT NULL DEFAULT 0,
                        game_in_progress INTEGER NOT NULL DEFAULT 0,
                        player_ids_json TEXT NOT NULL DEFAULT '[]'
                    );

                    CREATE TABLE IF NOT EXISTS gift_packages (
                        id BIGSERIAL PRIMARY KEY,
                        player_id BIGINT NOT NULL,
                        avatar_item_desc TEXT NOT NULL DEFAULT '',
                        xp INTEGER NOT NULL DEFAULT 0
                    );

                    CREATE TABLE IF NOT EXISTS kv_store (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS request_log (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        method TEXT NOT NULL,
                        path TEXT NOT NULL,
                        query_json TEXT NOT NULL,
                        status_code INTEGER NOT NULL DEFAULT 0,
                        note TEXT NOT NULL DEFAULT ''
                    );

                    CREATE TABLE IF NOT EXISTS websocket_events (
                        id BIGSERIAL PRIMARY KEY,
                        player_id BIGINT NOT NULL,
                        notification_id INTEGER NOT NULL,
                        payload_json TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS websocket_sessions (
                        player_id BIGINT NOT NULL,
                        session_id TEXT NOT NULL,
                        connected_at TEXT NOT NULL,
                        last_seen_at TEXT NOT NULL,
                        PRIMARY KEY (player_id, session_id)
                    );

                    CREATE TABLE IF NOT EXISTS player_reputation_events (
                        id BIGSERIAL PRIMARY KEY,
                        player_id BIGINT NOT NULL,
                        good_karma_minutes INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS player_scores (
                        id BIGSERIAL PRIMARY KEY,
                        player_id BIGINT NOT NULL,
                        session_id TEXT NOT NULL DEFAULT '',
                        activity TEXT NOT NULL DEFAULT '',
                        category TEXT NOT NULL DEFAULT '',
                        score DOUBLE PRECISION NOT NULL DEFAULT 0,
                        comment TEXT NOT NULL DEFAULT '',
                        secondary_score DOUBLE PRECISION,
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS player_reports (
                        id BIGSERIAL PRIMARY KEY,
                        reporter_player_id BIGINT NOT NULL,
                        reported_player_id BIGINT NOT NULL,
                        report_category INTEGER NOT NULL DEFAULT 0,
                        activity TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL
                    );
                    """
                )
                conn.execute(
                    "INSERT OR IGNORE INTO kv_store(key, value) VALUES('motd', ?)",
                    (DEFAULT_MOTD_TEXT,),
                )
                conn.execute("ALTER TABLE players ADD COLUMN IF NOT EXISTS developer INTEGER NOT NULL DEFAULT 0")
                conn.commit()
                INIT_COMPLETE = True
            except Exception:
                conn.rollback()
                raise
            finally:
                if advisory_lock_acquired:
                    try:
                        conn.execute("SELECT pg_advisory_unlock(?)", (DB_SCHEMA_LOCK_ID,))
                        conn.commit()
                    except Exception:
                        conn.rollback()

def safe_int(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return default
        return int(value)
    except Exception:
        return default


def parse_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_platform(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    raw = str(value).strip().lower()
    if raw.lstrip("-").isdigit():
        return int(raw)
    if raw == "steam":
        return 0
    if raw == "oculus":
        return 1
    return DEFAULT_PLATFORM


def stable_player_id(platform: int, platform_id: int) -> int:
    digest = hashlib.sha256(f"{platform}:{platform_id}".encode("utf-8")).digest()
    raw_value = int.from_bytes(digest[:8], "big") & 0x7FFFFFFF
    return raw_value or 1


def _player_id_candidates(platform: int, platform_id: int, preferred_player_id: int | None = None):
    seen: set[int] = set()

    def emit(candidate: int) -> int | None:
        candidate = safe_int(candidate, 0)
        if candidate <= 0 or candidate in seen:
            return None
        seen.add(candidate)
        return candidate

    preferred = emit(preferred_player_id or 0)
    if preferred is not None:
        yield preferred

    index = 0
    while True:
        seed = f"{platform}:{platform_id}:{index}"
        digest = hashlib.sha256(seed.encode("utf-8")).digest()
        candidate = emit((int.from_bytes(digest[:8], "big") & 0x7FFFFFFF) or 1)
        if candidate is not None:
            yield candidate
        index += 1


def _find_available_player_id(conn: PgConnection, platform: int, platform_id: int, preferred_player_id: int | None = None) -> int:
    for index, candidate in enumerate(_player_id_candidates(platform, platform_id, preferred_player_id)):
        row = conn.execute("SELECT id, platform, platform_id FROM players WHERE id = ?", (candidate,)).fetchone()
        if row is None:
            return candidate
        if safe_int(row.get("platform"), DEFAULT_PLATFORM) == platform and safe_int(row.get("platform_id"), 0) == platform_id:
            return candidate
        if index >= 63:
            break

    fallback = max(1, stable_player_id(platform, platform_id))
    for offset in range(1, 100000):
        candidate = ((fallback + offset) & 0x7FFFFFFF) or 1
        row = conn.execute("SELECT id FROM players WHERE id = ?", (candidate,)).fetchone()
        if row is None:
            return candidate
    raise RuntimeError("Unable to allocate a free player id")


def _payload_value(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return None


def _payload_string(payload: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        if key not in payload:
            continue
        value = str(payload[key] or "").strip()
        if value:
            return value
    return None


def _payload_optional_int(payload: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        if key in payload:
            return safe_int(payload[key], 0)
    return None


def player_response(row: Mapping[str, Any] | dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    level_value = max(1, safe_int(data.get("level", data.get("Level")), 1))
    return {
        "Id": safe_int(data.get("id", data.get("Id")), 0),
        "Platform": safe_int(data.get("platform", data.get("Platform")), DEFAULT_PLATFORM),
        "PlatformId": safe_int(data.get("platform_id", data.get("PlatformId")), 0),
        "Name": str(data.get("name", data.get("Name")) or DEFAULT_PLAYER_NAME),
        "DisplayName": str(data.get("display_name", data.get("DisplayName")) or data.get("name", DEFAULT_PLAYER_NAME)),
        "XP": max(0, safe_int(data.get("xp", data.get("XP")), 0)),
        "Level": level_value,
        "Reputation": safe_int(data.get("reputation", data.get("Reputation")), DEFAULT_REPUTATION),
        "Email": str(data.get("email", data.get("Email")) or DEFAULT_VERIFIED_EMAIL),
        "Username": str(data.get("username", data.get("Username")) or data.get("display_name", DEFAULT_PLAYER_NAME)),
        "Verified": bool(safe_int(data.get("verified", data.get("Verified")), 1)),
        "Developer": bool(safe_int(data.get("developer", data.get("Developer")), 0)),
        "XpRequiredToLevelUp": xp_required_for_level(level_value),
    }


def get_player_by_id(player_id: int) -> dict[str, Any] | None:
    init_db()
    with closing(connect()) as conn:
        row = conn.execute("SELECT * FROM players WHERE id = ?", (player_id,)).fetchone()
    return None if row is None else player_response(row)


def get_player_by_platform(platform: int, platform_id: int) -> dict[str, Any] | None:
    init_db()
    with closing(connect()) as conn:
        row = conn.execute(
            "SELECT * FROM players WHERE platform = ? AND platform_id = ?",
            (platform, platform_id),
        ).fetchone()
    return None if row is None else player_response(row)


def create_or_update_player(
    *,
    platform: int,
    platform_id: int,
    display_name: str | None = None,
    username: str | None = None,
    player_id: int | None = None,
    xp: int | None = None,
    level: int | None = None,
    reputation: int | None = None,
    developer: int | bool | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_db()
    payload = payload if isinstance(payload, dict) else {}

    requested_display_name = _payload_string(payload, "DisplayName", "displayName", "Name", "name")
    requested_username = _payload_string(payload, "Username", "username")
    requested_player_id = _payload_optional_int(payload, "Id", "id")
    requested_xp = _payload_optional_int(payload, "XP", "xp")
    requested_level = _payload_optional_int(payload, "Level", "level")
    requested_reputation = _payload_optional_int(payload, "Reputation", "reputation")

    if requested_display_name is None and display_name is not None:
        candidate = str(display_name or "").strip()
        requested_display_name = candidate or None
    if requested_username is None and username is not None:
        candidate = str(username or "").strip()
        requested_username = candidate or None
    if requested_player_id is None and player_id is not None and player_id > 0:
        requested_player_id = safe_int(player_id, 0)
    if requested_xp is None and xp is not None:
        requested_xp = safe_int(xp, 0)
    if requested_level is None and level is not None:
        requested_level = safe_int(level, 1)
    if requested_reputation is None and reputation is not None:
        requested_reputation = safe_int(reputation, DEFAULT_REPUTATION)

    developer_payload_value = _payload_value(payload, "Developer", "developer")
    if developer_payload_value is not None:
        requested_developer = 1 if parse_bool(developer_payload_value, False) else 0
    elif developer is not None:
        requested_developer = 1 if developer else 0
    else:
        requested_developer = None

    for attempt in range(3):
        with closing(connect()) as conn:
            try:
                existing_row = conn.execute(
                    "SELECT * FROM players WHERE platform = ? AND platform_id = ?",
                    (platform, platform_id),
                ).fetchone()

                if existing_row is not None:
                    existing = player_response(existing_row)
                    actual_player_id = safe_int(existing.get("Id"), 0)
                    final_display_name = requested_display_name or str(existing.get("DisplayName") or existing.get("Name") or DEFAULT_PLAYER_NAME)
                    final_username = requested_username or str(existing.get("Username") or final_display_name or DEFAULT_PLAYER_NAME)
                    final_xp = max(0, safe_int(existing.get("XP") if requested_xp is None else requested_xp, 0))
                    final_level = max(1, safe_int(existing.get("Level") if requested_level is None else requested_level, 1))
                    final_reputation = safe_int(existing.get("Reputation") if requested_reputation is None else requested_reputation, DEFAULT_REPUTATION)
                    final_developer = safe_int(existing.get("Developer") if requested_developer is None else requested_developer, 0)

                    conn.execute(
                        """
                        UPDATE players
                        SET
                            name = ?,
                            display_name = ?,
                            username = ?,
                            xp = ?,
                            level = ?,
                            reputation = ?,
                            email = ?,
                            verified = 1,
                            developer = ?
                        WHERE id = ?
                        """,
                        (
                            final_display_name,
                            final_display_name,
                            final_username,
                            final_xp,
                            final_level,
                            final_reputation,
                            DEFAULT_VERIFIED_EMAIL,
                            final_developer,
                            actual_player_id,
                        ),
                    )
                else:
                    actual_player_id = _find_available_player_id(conn, platform, platform_id, requested_player_id)
                    final_display_name = requested_display_name or DEFAULT_PLAYER_NAME
                    final_username = requested_username or final_display_name
                    final_xp = max(0, safe_int(DEFAULT_XP if requested_xp is None else requested_xp, 0))
                    final_level = max(1, safe_int(DEFAULT_LEVEL if requested_level is None else requested_level, 1))
                    final_reputation = safe_int(DEFAULT_REPUTATION if requested_reputation is None else requested_reputation, DEFAULT_REPUTATION)
                    final_developer = 0 if requested_developer is None else (1 if requested_developer else 0)

                    conn.execute(
                        """
                        INSERT INTO players(id, platform, platform_id, name, display_name, username, xp, level, reputation, email, verified, developer)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                        """,
                        (
                            actual_player_id,
                            platform,
                            platform_id,
                            final_display_name,
                            final_display_name,
                            final_username,
                            final_xp,
                            final_level,
                            final_reputation,
                            DEFAULT_VERIFIED_EMAIL,
                            final_developer,
                        ),
                    )

                conn.execute(
                    "INSERT OR IGNORE INTO avatars(player_id, outfit_selections, skin_color, hair_color) VALUES(?, '', '', '')",
                    (actual_player_id,),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO presence(
                        player_id, game_session_id, app_version, last_update_time, activity, private, available_space, game_in_progress
                    ) VALUES (?, '', '', ?, 'DormRoom', 0, 0, 0)
                    """,
                    (actual_player_id, utcnow_iso()),
                )
                conn.commit()
                return get_player_by_id(actual_player_id)  # type: ignore[return-value]
            except psycopg.IntegrityError:
                conn.rollback()
                if attempt >= 2:
                    raise

    raise RuntimeError("Failed to create or update player")

def list_players_by_ids(player_ids: list[int]) -> list[dict[str, Any]]:
    unique_ids = [pid for pid in dict.fromkeys(pid for pid in player_ids if pid > 0)]
    if not unique_ids:
        return []
    init_db()
    placeholders = ",".join("?" for _ in unique_ids)
    with closing(connect()) as conn:
        rows = conn.execute(f"SELECT * FROM players WHERE id IN ({placeholders})", tuple(unique_ids)).fetchall()
    by_id = {safe_int(row["id"], 0): player_response(row) for row in rows}
    return [by_id[pid] for pid in unique_ids if pid in by_id]


def set_reputation(player_id: int, reputation: int) -> dict[str, Any]:
    init_db()
    existing = get_player_by_id(player_id)
    if existing is None:
        player = create_or_update_player(platform=DEFAULT_PLATFORM, platform_id=player_id, player_id=player_id)
    else:
        player = existing
    with closing(connect()) as conn:
        conn.execute("UPDATE players SET reputation = ? WHERE id = ?", (reputation, player["Id"]))
        conn.commit()
    return get_player_by_id(player["Id"])  # type: ignore[return-value]


def xp_required_for_level(level: int) -> int:
    level = max(1, safe_int(level, 1))
    return 500 + ((level - 1) * 250)


OBJECTIVE_XP = {
    100: 100, 101: 100, 200: 100, 201: 100, 202: 100, 300: 100, 301: 100, 302: 100,
    400: 100, 402: 100, 500: 100, 501: 100, 502: 100, 603: 100, 701: 100, 702: 100,
    801: 100, 802: 100,
}


def apply_objective_completion(player_id: int, objective_type: int, additional_xp: int = 0, in_party: bool = False) -> dict[str, int]:
    player = get_player_by_id(player_id)
    if player is None:
        player = create_or_update_player(platform=DEFAULT_PLATFORM, platform_id=player_id, player_id=player_id)

    delta_xp = max(25, OBJECTIVE_XP.get(safe_int(objective_type, 0), 100) + max(0, safe_int(additional_xp, 0)))
    if in_party:
        delta_xp += 25

    current_xp = max(0, safe_int(player["XP"], 0)) + delta_xp
    current_level = max(1, safe_int(player["Level"], 1))
    threshold = xp_required_for_level(current_level)
    while current_xp >= threshold:
        current_xp -= threshold
        current_level += 1
        threshold = xp_required_for_level(current_level)

    with closing(connect()) as conn:
        conn.execute("UPDATE players SET xp = ?, level = ? WHERE id = ?", (current_xp, current_level, player_id))
        conn.commit()

    return {
        "deltaXp": delta_xp,
        "currentLevel": current_level,
        "currentXp": current_xp,
        "xpRequiredToLevelUp": threshold,
    }


def get_settings(player_id: int) -> list[dict[str, str]]:
    init_db()
    with closing(connect()) as conn:
        rows = conn.execute("SELECT key, value FROM settings WHERE player_id = ? ORDER BY key", (player_id,)).fetchall()
    return [{"Key": str(row["key"]), "Value": str(row["value"])} for row in rows]


def upsert_setting(player_id: int, key: str, value: str) -> None:
    init_db()
    with closing(connect()) as conn:
        conn.execute(
            "INSERT INTO settings(player_id, key, value) VALUES(?, ?, ?) ON CONFLICT(player_id, key) DO UPDATE SET value = excluded.value",
            (player_id, key, value),
        )
        conn.commit()


def delete_setting(player_id: int, key: str) -> None:
    init_db()
    with closing(connect()) as conn:
        conn.execute("DELETE FROM settings WHERE player_id = ? AND key = ?", (player_id, key))
        conn.commit()


def get_avatar(player_id: int) -> dict[str, str]:
    init_db()
    with closing(connect()) as conn:
        row = conn.execute("SELECT * FROM avatars WHERE player_id = ?", (player_id,)).fetchone()
        if row is None:
            conn.execute("INSERT OR IGNORE INTO avatars(player_id, outfit_selections, skin_color, hair_color) VALUES(?, '', '', '')", (player_id,))
            conn.commit()
            row = conn.execute("SELECT * FROM avatars WHERE player_id = ?", (player_id,)).fetchone()
    return {
        "OutfitSelections": str(row["outfit_selections"]) if row else "",
        "SkinColor": str(row["skin_color"]) if row else "",
        "HairColor": str(row["hair_color"]) if row else "",
    }


def set_avatar(player_id: int, payload: dict[str, Any]) -> dict[str, str]:
    current = get_avatar(player_id)
    outfit = str(payload.get("OutfitSelections", current["OutfitSelections"]) or "")
    skin = str(payload.get("SkinColor", current["SkinColor"]) or "")
    hair = str(payload.get("HairColor", current["HairColor"]) or "")
    with closing(connect()) as conn:
        conn.execute(
            """
            INSERT INTO avatars(player_id, outfit_selections, skin_color, hair_color)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(player_id) DO UPDATE SET
                outfit_selections = excluded.outfit_selections,
                skin_color = excluded.skin_color,
                hair_color = excluded.hair_color
            """,
            (player_id, outfit, skin, hair),
        )
        conn.commit()
    return get_avatar(player_id)


def get_avatar_items(player_id: int) -> list[dict[str, Any]]:
    init_db()
    with closing(connect()) as conn:
        rows = conn.execute(
            "SELECT avatar_item_desc, unlocked_level FROM avatar_items WHERE player_id = ? ORDER BY avatar_item_desc",
            (player_id,),
        ).fetchall()
    return [{"AvatarItemDesc": str(row["avatar_item_desc"]), "UnlockedLevel": max(1, safe_int(row["unlocked_level"], 1))} for row in rows]


def add_avatar_item(player_id: int, avatar_item_desc: str, unlocked_level: int = 1) -> dict[str, Any]:
    init_db()
    with closing(connect()) as conn:
        conn.execute(
            """
            INSERT INTO avatar_items(player_id, avatar_item_desc, unlocked_level)
            VALUES (?, ?, ?)
            ON CONFLICT(player_id, avatar_item_desc) DO UPDATE SET unlocked_level = MAX(unlocked_level, excluded.unlocked_level)
            """,
            (player_id, avatar_item_desc, max(1, safe_int(unlocked_level, 1))),
        )
        conn.commit()
    return {"AvatarItemDesc": avatar_item_desc, "UnlockedLevel": max(1, safe_int(unlocked_level, 1))}


def get_presence(player_id: int) -> dict[str, Any] | None:
    init_db()
    with closing(connect()) as conn:
        row = conn.execute("SELECT * FROM presence WHERE player_id = ?", (player_id,)).fetchone()
    if row is None:
        return None
    return {
        "PlayerId": safe_int(row["player_id"], 0),
        "GameSessionId": str(row["game_session_id"] or ""),
        "AppVersion": str(row["app_version"] or ""),
        "LastUpdateTime": parse_dotnet_ticks(row["last_update_time"]),
        "Activity": str(row["activity"] or "DormRoom"),
        "Private": bool(row["private"]),
        "AvailableSpace": max(0, safe_int(row["available_space"], 0)),
        "GameInProgress": bool(row["game_in_progress"]),
    }


def set_presence(player_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    current = get_presence(player_id) or {
        "PlayerId": player_id,
        "GameSessionId": "",
        "AppVersion": "",
        "LastUpdateTime": utcnow_ticks(),
        "Activity": "DormRoom",
        "Private": False,
        "AvailableSpace": 0,
        "GameInProgress": False,
    }
    value = {
        "PlayerId": player_id,
        "GameSessionId": str(payload.get("GameSessionId", payload.get("gameSessionId", current["GameSessionId"])) or ""),
        "AppVersion": str(payload.get("AppVersion", payload.get("appVersion", current["AppVersion"])) or ""),
        # Presence freshness must be stamped by the server on every heartbeat.
        # The client may omit LastUpdateTime or keep sending an older value, which
        # would make active-player counts drift low even while /api/presence/v2 is
        # still arriving regularly.
        "LastUpdateTime": utcnow_ticks(),
        "Activity": str(payload.get("Activity", payload.get("activity", current["Activity"])) or "DormRoom"),
        "Private": parse_bool(payload.get("Private", payload.get("private", current["Private"])), False),
        "AvailableSpace": max(0, safe_int(payload.get("AvailableSpace", payload.get("availableSpace", current["AvailableSpace"])), 0)),
        "GameInProgress": parse_bool(payload.get("GameInProgress", payload.get("gameInProgress", current["GameInProgress"])), False),
    }
    with closing(connect()) as conn:
        conn.execute(
            """
            INSERT INTO presence(player_id, game_session_id, app_version, last_update_time, activity, private, available_space, game_in_progress)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_id) DO UPDATE SET
                game_session_id=excluded.game_session_id,
                app_version=excluded.app_version,
                last_update_time=excluded.last_update_time,
                activity=excluded.activity,
                private=excluded.private,
                available_space=excluded.available_space,
                game_in_progress=excluded.game_in_progress
            """,
            (
                player_id,
                value["GameSessionId"],
                value["AppVersion"],
                value["LastUpdateTime"],
                value["Activity"],
                int(value["Private"]),
                value["AvailableSpace"],
                int(value["GameInProgress"]),
            ),
        )
        conn.commit()
    return value


def list_presence(player_ids: list[int]) -> list[dict[str, Any]]:
    return [presence for player_id in dict.fromkeys(pid for pid in player_ids if pid > 0) if (presence := get_presence(player_id)) is not None]


def get_relationships(player_id: int) -> list[dict[str, Any]]:
    init_db()
    with closing(connect()) as conn:
        rows = conn.execute(
            "SELECT other_player_id, relationship_type FROM relationships WHERE player_id = ? AND other_player_id <> ? ORDER BY other_player_id",
            (player_id, player_id),
        ).fetchall()
    return [{"PlayerID": safe_int(row["other_player_id"], 0), "RelationshipType": safe_int(row["relationship_type"], 0)} for row in rows]


def set_relationship(player_id: int, other_player_id: int, relationship_type: int) -> dict[str, Any]:
    init_db()
    with closing(connect()) as conn:
        conn.execute(
            """
            INSERT INTO relationships(player_id, other_player_id, relationship_type)
            VALUES (?, ?, ?)
            ON CONFLICT(player_id, other_player_id) DO UPDATE SET relationship_type = excluded.relationship_type
            """,
            (player_id, other_player_id, relationship_type),
        )
        conn.commit()
    relation = {"PlayerID": other_player_id, "RelationshipType": relationship_type}
    enqueue_ws_event(player_id, 1, relation)
    return relation


def apply_relationship_action(action: str, id1: int, id2: int) -> dict[str, Any]:
    action = action.lower().strip()
    if id1 <= 0 or id2 <= 0 or id1 == id2:
        return {"PlayerID": safe_int(id2, 0), "RelationshipType": 0}
    if action == "addfriend":
        local_relation = set_relationship(id1, id2, 3)
        set_relationship(id2, id1, 3)
        return local_relation
    if action == "removefriend":
        local_relation = set_relationship(id1, id2, 0)
        set_relationship(id2, id1, 0)
        return local_relation
    if action == "sendfriendrequest":
        local_relation = set_relationship(id1, id2, 1)
        set_relationship(id2, id1, 2)
        return local_relation
    if action == "acceptfriendrequest":
        local_relation = set_relationship(id1, id2, 3)
        set_relationship(id2, id1, 3)
        return local_relation
    if action == "blockplayer":
        local_relation = set_relationship(id1, id2, 4)
        set_relationship(id2, id1, 5)
        return local_relation
    if action == "unblockplayer":
        local_relation = set_relationship(id1, id2, 0)
        set_relationship(id2, id1, 0)
        return local_relation
    return set_relationship(id1, id2, 0)


def create_message(from_player_id: int, to_player_id: int, msg_type: int, data: str = "") -> dict[str, Any]:
    init_db()
    with closing(connect()) as conn:
        cursor = conn.execute(
            "INSERT INTO messages(from_player_id, to_player_id, sent_time, type, data) VALUES (?, ?, ?, ?, ?) RETURNING id",
            (from_player_id, to_player_id, str(utcnow_ticks()), msg_type, str(data or "")),
        )
        inserted = cursor.fetchone()
        message_id = safe_int(inserted["id"] if inserted is not None else 0, 0)
        conn.commit()
    message = {
        "Id": message_id,
        "FromPlayerId": from_player_id,
        "ToPlayerId": to_player_id,
        "SentTime": utcnow_ticks(),
        "Type": msg_type,
        "Data": str(data or ""),
    }
    enqueue_ws_event(to_player_id, 2, {k: v for k, v in message.items() if k != "ToPlayerId"})
    return message


def get_messages_for_player(player_id: int) -> list[dict[str, Any]]:
    init_db()
    with closing(connect()) as conn:
        rows = conn.execute(
            "SELECT id, from_player_id, to_player_id, sent_time, type, data FROM messages WHERE to_player_id = ? ORDER BY id",
            (player_id,),
        ).fetchall()
    return [
        {
            "Id": safe_int(row["id"], 0),
            "FromPlayerId": safe_int(row["from_player_id"], 0),
            "SentTime": parse_dotnet_ticks(row["sent_time"]),
            "Type": safe_int(row["type"], 0),
            "Data": str(row["data"] or ""),
        }
        for row in rows
    ]


def delete_message(message_id: int) -> bool:
    init_db()
    with closing(connect()) as conn:
        row = conn.execute("SELECT to_player_id FROM messages WHERE id = ?", (message_id,)).fetchone()
        if row is None:
            return False
        to_player_id = safe_int(row["to_player_id"], 0)
        conn.execute("DELETE FROM messages WHERE id = ?", (message_id,))
        conn.commit()
    enqueue_ws_event(to_player_id, 3, {"Id": message_id})
    return True


def record_good_karma(player_id: int, good_karma_minutes: int) -> dict[str, Any]:
    init_db()
    normalized_minutes = max(0, safe_int(good_karma_minutes, 0))
    with closing(connect()) as conn:
        conn.execute(
            "INSERT INTO player_reputation_events(player_id, good_karma_minutes, created_at) VALUES (?, ?, ?)",
            (player_id, normalized_minutes, utcnow_iso()),
        )
        conn.commit()
    return {"PlayerId": player_id, "GoodKarmaMinutes": normalized_minutes}


def record_player_score(
    player_id: int,
    session_id: str,
    activity: str,
    category: str,
    score: float,
    comment: str = "",
    secondary_score: float | None = None,
) -> dict[str, Any]:
    init_db()
    normalized_score = float(score)
    normalized_secondary = None if secondary_score is None else float(secondary_score)
    with closing(connect()) as conn:
        conn.execute(
            """
            INSERT INTO player_scores(player_id, session_id, activity, category, score, comment, secondary_score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                str(session_id or ""),
                str(activity or ""),
                str(category or ""),
                normalized_score,
                str(comment or ""),
                normalized_secondary,
                utcnow_iso(),
            ),
        )
        conn.commit()
    return {
        "PlayerId": player_id,
        "SessionId": str(session_id or ""),
        "Activity": str(activity or ""),
        "Category": str(category or ""),
        "Score": normalized_score,
        "Comment": str(comment or ""),
        "SecondaryScore": normalized_secondary,
    }


def record_player_report(reporter_player_id: int, reported_player_id: int, report_category: int, activity: str) -> dict[str, Any]:
    init_db()
    with closing(connect()) as conn:
        conn.execute(
            """
            INSERT INTO player_reports(reporter_player_id, reported_player_id, report_category, activity, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (reporter_player_id, reported_player_id, safe_int(report_category, 0), str(activity or ""), utcnow_iso()),
        )
        conn.commit()
    return {
        "ReporterPlayerId": reporter_player_id,
        "PlayerIdReported": reported_player_id,
        "ReportCategory": safe_int(report_category, 0),
        "Activity": str(activity or ""),
    }


def get_game_sessions(app_version: str = "") -> list[dict[str, Any]]:
    init_db()
    query = "SELECT * FROM game_sessions"
    params: tuple[Any, ...] = ()
    if app_version:
        query += " WHERE app_version = ?"
        params = (app_version,)
    query += " ORDER BY id"
    with closing(connect()) as conn:
        rows = conn.execute(query, params).fetchall()
    result = []
    for row in rows:
        result.append({
            "Id": str(row["id"]),
            "AppVersion": str(row["app_version"] or ""),
            "Activity": str(row["activity"] or "DormRoom"),
            "Private": bool(row["private"]),
            "AvailableSpace": max(0, safe_int(row["available_space"], 0)),
            "GameInProgress": bool(row["game_in_progress"]),
            "PlayerIds": json.loads(str(row["player_ids_json"] or "[]")),
        })
    return result


def get_game_session(session_id: str) -> dict[str, Any] | None:
    init_db()
    with closing(connect()) as conn:
        row = conn.execute("SELECT * FROM game_sessions WHERE id = ?", (session_id,)).fetchone()
    if row is None:
        return None
    return {
        "Id": str(row["id"]),
        "AppVersion": str(row["app_version"] or ""),
        "Activity": str(row["activity"] or "DormRoom"),
        "Private": bool(row["private"]),
        "AvailableSpace": max(0, safe_int(row["available_space"], 0)),
        "GameInProgress": bool(row["game_in_progress"]),
        "PlayerIds": json.loads(str(row["player_ids_json"] or "[]")),
    }


def get_gift_packages(player_id: int) -> list[dict[str, Any]]:
    init_db()
    with closing(connect()) as conn:
        rows = conn.execute("SELECT id, avatar_item_desc, xp FROM gift_packages WHERE player_id = ? ORDER BY id", (player_id,)).fetchall()
    return [{"Id": safe_int(row["id"], 0), "AvatarItemDesc": str(row["avatar_item_desc"] or ""), "Xp": max(0, safe_int(row["xp"], 0))} for row in rows]


def create_gift_package(player_id: int, avatar_item_desc: str, xp: int) -> dict[str, Any]:
    init_db()
    with closing(connect()) as conn:
        cursor = conn.execute(
            "INSERT INTO gift_packages(player_id, avatar_item_desc, xp) VALUES (?, ?, ?) RETURNING id",
            (player_id, str(avatar_item_desc or ""), max(0, safe_int(xp, 0))),
        )
        inserted = cursor.fetchone()
        gift_id = safe_int(inserted["id"] if inserted is not None else 0, 0)
        conn.commit()
    return {"Id": gift_id, "AvatarItemDesc": str(avatar_item_desc or ""), "Xp": max(0, safe_int(xp, 0))}


def consume_gift_package(player_id: int, gift_id: int) -> bool:
    init_db()
    with closing(connect()) as conn:
        cursor = conn.execute("DELETE FROM gift_packages WHERE player_id = ? AND id = ?", (player_id, gift_id))
        conn.commit()
    return safe_int(cursor.rowcount, 0) > 0


def get_motd() -> str:
    init_db()
    with closing(connect()) as conn:
        row = conn.execute("SELECT value FROM kv_store WHERE key = 'motd'").fetchone()
    return DEFAULT_MOTD_TEXT if row is None else str(row["value"] or DEFAULT_MOTD_TEXT)


def set_motd(value: str) -> str:
    init_db()
    with closing(connect()) as conn:
        conn.execute(
            "INSERT INTO kv_store(key, value) VALUES('motd', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (value,),
        )
        conn.commit()
    return get_motd()


def enqueue_ws_event(player_id: int, notification_id: int, payload: Any) -> int:
    init_db()
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    with closing(connect()) as conn:
        cursor = conn.execute(
            "INSERT INTO websocket_events(player_id, notification_id, payload_json, created_at) VALUES (?, ?, ?, ?) RETURNING id",
            (player_id, notification_id, payload_json, utcnow_iso()),
        )
        inserted = cursor.fetchone()
        event_id = safe_int(inserted["id"] if inserted is not None else 0, 0)
        conn.commit()
    return event_id


def list_ws_events_since(player_id: int, after_event_id: int) -> list[dict[str, Any]]:
    init_db()
    with closing(connect()) as conn:
        rows = conn.execute(
            """
            SELECT id, notification_id, payload_json, created_at
            FROM websocket_events
            WHERE player_id = ? AND id > ?
            ORDER BY id
            """,
            (player_id, after_event_id),
        ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"]))
        except Exception:
            payload = str(row["payload_json"])
        result.append({
            "EventId": safe_int(row["id"], 0),
            "NotificationId": safe_int(row["notification_id"], 0),
            "Payload": payload,
            "CreatedAt": str(row["created_at"]),
        })
    return result


def touch_ws_session(player_id: int, session_id: str) -> None:
    init_db()
    now = utcnow_iso()
    with closing(connect()) as conn:
        conn.execute(
            """
            INSERT INTO websocket_sessions(player_id, session_id, connected_at, last_seen_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(player_id, session_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
            """,
            (player_id, session_id, now, now),
        )
        conn.commit()


def remove_ws_session(player_id: int, session_id: str) -> None:
    init_db()
    with closing(connect()) as conn:
        conn.execute("DELETE FROM websocket_sessions WHERE player_id = ? AND session_id = ?", (player_id, session_id))
        conn.commit()


def list_ws_sessions() -> list[dict[str, Any]]:
    init_db()
    with closing(connect()) as conn:
        rows = conn.execute("SELECT player_id, session_id, connected_at, last_seen_at FROM websocket_sessions ORDER BY player_id, session_id").fetchall()
    return [
        {
            "PlayerId": safe_int(row["player_id"], 0),
            "SessionId": str(row["session_id"]),
            "ConnectedAt": str(row["connected_at"]),
            "LastSeenAt": str(row["last_seen_at"]),
        }
        for row in rows
    ]


def constant_time_auth_compare(provided: str, expected: str) -> bool:
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))


def auth_header_valid(authorization: str | None) -> bool:
    if not authorization:
        return False
    if not authorization.startswith("Basic "):
        return False
    try:
        raw_value = base64.b64decode(authorization[6:].strip()).decode("utf-8")
    except Exception:
        return False
    username, separator, password = raw_value.partition(":")
    if not separator:
        return False
    return constant_time_auth_compare(username, AUTH_USERNAME) and constant_time_auth_compare(password, AUTH_PASSWORD)


def log_request(method: str, path: str, query: dict[str, Any] | None = None, status_code: int = 0, note: str = "") -> None:
    init_db()
    with closing(connect()) as conn:
        conn.execute(
            "INSERT INTO request_log(created_at, method, path, query_json, status_code, note) VALUES (?, ?, ?, ?, ?, ?)",
            (utcnow_iso(), method, path, json.dumps(query or {}, separators=(",", ":")), status_code, note),
        )
        conn.commit()


def list_recent_requests(limit: int = 200) -> list[dict[str, Any]]:
    init_db()
    with closing(connect()) as conn:
        rows = conn.execute(
            "SELECT created_at, method, path, query_json, status_code, note FROM request_log ORDER BY id DESC LIMIT ?",
            (max(1, limit),),
        ).fetchall()
    result = []
    for row in rows:
        try:
            query = json.loads(str(row["query_json"]))
        except Exception:
            query = {}
        result.append({
            "CreatedAt": str(row["created_at"]),
            "Method": str(row["method"]),
            "Path": str(row["path"]),
            "Query": query,
            "StatusCode": safe_int(row["status_code"], 0),
            "Note": str(row["note"] or ""),
        })
    return result



def get_presence_snapshot(stale_timeout_seconds: int = 180) -> dict[str, Any]:
    init_db()
    stale_timeout_seconds = max(60, safe_int(stale_timeout_seconds, 180))
    now = datetime.now(timezone.utc)
    active_players = 0
    players_in_rooms = 0
    with closing(connect()) as conn:
        rows = conn.execute("SELECT player_id, game_session_id, last_update_time FROM presence ORDER BY player_id").fetchall()
    for row in rows:
        last_update_ticks = parse_dotnet_ticks(row['last_update_time'])
        last_update_dt = datetime.fromtimestamp(max(0, (last_update_ticks - 621355968000000000) / 10_000_000), tz=timezone.utc)
        if (now - last_update_dt).total_seconds() > stale_timeout_seconds:
            continue
        active_players += 1
        if str(row['game_session_id'] or '').strip():
            players_in_rooms += 1
    return {
        'active_players': active_players,
        'players_in_rooms': players_in_rooms,
        'stale_timeout_seconds': stale_timeout_seconds,
    }


def get_health_snapshot(window_seconds: int = 300) -> dict[str, Any]:
    init_db()
    window_seconds = max(60, safe_int(window_seconds, 300))
    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - window_seconds
    total_requests = 0
    counted_errors = 0
    server_error_count = 0
    with closing(connect()) as conn:
        rows = conn.execute("SELECT created_at, method, path, status_code, note FROM request_log ORDER BY id DESC").fetchall()
    for row in rows:
        created_raw = str(row['created_at'] or '')
        try:
            created_at = datetime.fromisoformat(created_raw.replace('Z', '+00:00'))
        except Exception:
            continue
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        else:
            created_at = created_at.astimezone(timezone.utc)
        if created_at.timestamp() < cutoff:
            break
        path = str(row['path'] or '')
        method = str(row['method'] or '')
        status_code = safe_int(row['status_code'], 0)
        note = str(row['note'] or '')
        if method == 'WS' and note == 'connect-attempt':
            total_requests += 1
        elif method != 'WS':
            total_requests += 1
        if path == '/favicon.ico':
            continue
        if status_code >= 500:
            counted_errors += 1
            server_error_count += 1
    error_rate_percent = round((counted_errors / total_requests) * 100.0, 2) if total_requests > 0 else 0.0
    return {
        'window_seconds': window_seconds,
        'total_requests': total_requests,
        'counted_errors': counted_errors,
        'error_rate_percent': error_rate_percent,
        'missing_http_count': 0,
        'missing_ws_count': 0,
        'server_error_count': server_error_count,
    }
