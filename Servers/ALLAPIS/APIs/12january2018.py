"""RecNet HTTP surface for the 12 January 2018 build.

Confirmed from first-party non-Photon client code in Assembly-CSharp.dll.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

from fastapi import HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response


API_VERSION = "12january2018"
_DEFAULT_AMPLITUDE_KEY = "f1779b982f1c09aed3adb3cca563cbc2"
_DEFAULT_OUTFIT_SELECTIONS = ""
_DEFAULT_SKIN_COLOR = ""
_DEFAULT_HAIR_COLOR = ""
# 20180112 parses ActivityLevelId through the client ActivityRuntimeConfig.
# These are the serialized ActivityLevel.Id GUIDs embedded in resources.assets.
_DORM_ACTIVITY_LEVEL_ID = "76d98498-60a1-430c-ab76-b54a29b7a163"
_ACTIVITY_LEVEL_DISPLAY_NAMES = {
    _DORM_ACTIVITY_LEVEL_ID: "Dorm Room",
    "cbad71af-0831-44d8-b8ef-69edafa841f6": "Rec Center",
    "4078dfed-24bb-4db7-863f-578ba48d726b": "Charades",
    "f6f7256c-e438-4299-b99e-d20bef8cf7e0": "Lake",
    "d9378c9f-80bc-46fb-ad1e-1bed8a674f55": "Propulsion",
    "3d474b26-26f7-45e9-9a36-9b02847d5e6f": "Dodgeball",
    "a067557f-ca32-43e6-b6e5-daaec60b4f5a": "The Lounge",
    "d89f74fa-d51e-477a-a425-025a891dd499": "Paddleball",
    "e122fe98-e7db-49e8-a1b1-105424b6e1f0": "River",
    "a785267d-c579-42ea-be43-fec1992d1ca7": "Homestead",
    "ff4c6427-7079-4f59-b22a-69b089420827": "Quarry",
    "380d18b5-de9c-49f3-80f7-f4a95c1de161": "Clear Cut",
    "58763055-2dfb-4814-80b8-16fac5c85709": "Spillway",
    "91e16e35-f48f-4700-ab8a-a1b79e50e51b": "Quest For The Golden Trophy",
    "acc06e66-c2d0-4361-b0cd-46246a4c455c": "The Rise Of JumboTron",
    "949fa41f-4347-45c0-b7ac-489129174045": "Curse of the Crimson Cauldron",
    "7e01cfe0-820a-406f-b1b3-0a5bf575235c": "The Isle of Lost Skulls",
    "6d5eea4b-f069-4ed0-9916-0e2f07df0d03": "Soccer",
    "42699ed2-0c1b-4f3d-93a2-ce01dfce7a79": "Art Testing",
    "03a2f8aa-1cdc-4b11-8783-87aaa3713bad": "Art Testing Perf",
    "7ef6a766-0e3d-4671-bc6f-69c0e081e94b": "Dorm Photo Studio",
    "9932f88f-3929-43a0-a012-a40b5128e346": "Performance Hall",
    "f5fbd9c9-e853-4036-9d48-5f68e861af04": "Room Calibration",
    "0a864c86-5a71-4e18-8041-8124e4dc9d98": "Park",
    "239e676c-f12f-489f-bf3a-d4c383d692c3": "Warehouse",
    "a75f7547-79eb-47c6-8986-6767abcb4f92": "Registration",
}
_KNOWN_ACTIVITY_LEVEL_IDS = set(_ACTIVITY_LEVEL_DISPLAY_NAMES)
_KNOWN_ACTIVITY_LEVEL_IDS_CASEFOLD = {item.casefold(): item for item in _KNOWN_ACTIVITY_LEVEL_IDS}
_ACTIVITY_LEVEL_ALIASES = {
    "": _DORM_ACTIVITY_LEVEL_ID,
    "dorm": _DORM_ACTIVITY_LEVEL_ID,
    "dormroom": _DORM_ACTIVITY_LEVEL_ID,
    "dormroomscene": _DORM_ACTIVITY_LEVEL_ID,
    "reccenter": "cbad71af-0831-44d8-b8ef-69edafa841f6",
    "recroom": "cbad71af-0831-44d8-b8ef-69edafa841f6",
    "lounge": "a067557f-ca32-43e6-b6e5-daaec60b4f5a",
    "thelounge": "a067557f-ca32-43e6-b6e5-daaec60b4f5a",
    "charades": "4078dfed-24bb-4db7-863f-578ba48d726b",
    "discgolf": "f6f7256c-e438-4299-b99e-d20bef8cf7e0",
    "discgolflake": "f6f7256c-e438-4299-b99e-d20bef8cf7e0",
    "lake": "f6f7256c-e438-4299-b99e-d20bef8cf7e0",
    "propulsion": "d9378c9f-80bc-46fb-ad1e-1bed8a674f55",
    "dodgeball": "3d474b26-26f7-45e9-9a36-9b02847d5e6f",
    "paddleball": "d89f74fa-d51e-477a-a425-025a891dd499",
    "paintball": "e122fe98-e7db-49e8-a1b1-105424b6e1f0",
    "paintballriver": "e122fe98-e7db-49e8-a1b1-105424b6e1f0",
    "river": "e122fe98-e7db-49e8-a1b1-105424b6e1f0",
    "paintballhomestead": "a785267d-c579-42ea-be43-fec1992d1ca7",
    "homestead": "a785267d-c579-42ea-be43-fec1992d1ca7",
    "paintballquarry": "ff4c6427-7079-4f59-b22a-69b089420827",
    "quarry": "ff4c6427-7079-4f59-b22a-69b089420827",
    "paintballclearcut": "380d18b5-de9c-49f3-80f7-f4a95c1de161",
    "clearcut": "380d18b5-de9c-49f3-80f7-f4a95c1de161",
    "paintballdam": "58763055-2dfb-4814-80b8-16fac5c85709",
    "dam": "58763055-2dfb-4814-80b8-16fac5c85709",
    "spillway": "58763055-2dfb-4814-80b8-16fac5c85709",
    "capturetheflag": "e122fe98-e7db-49e8-a1b1-105424b6e1f0",
    "paintballcapturetheflag": "e122fe98-e7db-49e8-a1b1-105424b6e1f0",
    "quest": "91e16e35-f48f-4700-ab8a-a1b79e50e51b",
    "questforthegoldentrophy": "91e16e35-f48f-4700-ab8a-a1b79e50e51b",
    "goldentrophy": "91e16e35-f48f-4700-ab8a-a1b79e50e51b",
    "questgoblina": "91e16e35-f48f-4700-ab8a-a1b79e50e51b",
    "jumbotron": "acc06e66-c2d0-4361-b0cd-46246a4c455c",
    "theriseofjumbotron": "acc06e66-c2d0-4361-b0cd-46246a4c455c",
    "crimsoncauldron": "949fa41f-4347-45c0-b7ac-489129174045",
    "curseofthecrimsoncauldron": "949fa41f-4347-45c0-b7ac-489129174045",
    "isleoflostskulls": "7e01cfe0-820a-406f-b1b3-0a5bf575235c",
    "theisleoflostskulls": "7e01cfe0-820a-406f-b1b3-0a5bf575235c",
    "soccer": "6d5eea4b-f069-4ed0-9916-0e2f07df0d03",
    "arttesting": "42699ed2-0c1b-4f3d-93a2-ce01dfce7a79",
    "arttestingperf": "03a2f8aa-1cdc-4b11-8783-87aaa3713bad",
    "dormphotostudio": "7ef6a766-0e3d-4671-bc6f-69c0e081e94b",
    "performancehall": "9932f88f-3929-43a0-a012-a40b5128e346",
    "roomcalibration": "f5fbd9c9-e853-4036-9d48-5f68e861af04",
    "park": "0a864c86-5a71-4e18-8041-8124e4dc9d98",
    "lasertag": "239e676c-f12f-489f-bf3a-d4c383d692c3",
    "warehouse": "239e676c-f12f-489f-bf3a-d4c383d692c3",
    "registration": "a75f7547-79eb-47c6-8986-6767abcb4f92",
}
_CHARADES_WORDS = [
    {"EN_US": "Basketball", "Difficulty": 0},
    {"EN_US": "Robot", "Difficulty": 0},
    {"EN_US": "Airplane", "Difficulty": 0},
    {"EN_US": "Pirate", "Difficulty": 0},
    {"EN_US": "Dragon", "Difficulty": 1},
    {"EN_US": "Guitar", "Difficulty": 0},
    {"EN_US": "Juggling", "Difficulty": 1},
    {"EN_US": "Treasure Chest", "Difficulty": 1},
]

_KNOWN_UNIMPLEMENTED_PREFIXES: tuple[str, ...] = ()


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _now_ticks() -> int:
    return int(time.time() * 10_000_000) + 621355968000000000


def _safe_username(value: str | None, fallback: str) -> str:
    value = (value or "").strip() or fallback
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^A-Za-z0-9_.-]", "", value)
    return value[:32] or fallback


def _is_reserved_coach_name(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return text.casefold() == "coach" or _safe_username(text, "").casefold() == "coach"


async def _body_bytes(request: Request) -> bytes:
    try:
        return await request.body()
    except Exception:
        return b""


async def _json_body(request: Request, default: Any) -> Any:
    body = await _body_bytes(request)
    if not body:
        return default
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        parsed = parse_qs(body.decode("utf-8", errors="ignore"), keep_blank_values=True)
        if parsed:
            return {key: values[-1] if values else "" for key, values in parsed.items()}
        return default


def _int_value(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _bool_value(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().casefold()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def _trace_enabled() -> bool:
    return str(os.environ.get("RECNET_TRACE") or "").strip().casefold() in {"1", "true", "yes", "on"}


def _trace_recnet(context: Any, event: str, **fields: Any) -> None:
    if not _trace_enabled():
        return
    try:
        data_dir = Path(getattr(context, "data_dir", "DATA"))
        trace_dir = data_dir / "DEBUG"
        trace_dir.mkdir(parents=True, exist_ok=True)
        record = {
            "at": _now_iso(),
            "api_version": API_VERSION,
            "event": event,
            **fields,
        }
        with (trace_dir / "20180112_recnet_trace.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
    except Exception:
        return


def _image_type(content: bytes) -> tuple[str, str] | None:
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png", "image/png"
    if content.startswith(b"\xff\xd8\xff"):
        return ".jpg", "image/jpeg"
    return None


def _multipart_field(body: bytes, content_type: str, field_name: str) -> bytes | None:
    match = re.search(r'boundary="?([^";]+)"?', content_type)
    if not match:
        return None
    boundary = b"--" + match.group(1).encode("utf-8")
    for raw_part in body.split(boundary):
        part = raw_part.strip()
        if not part or part == b"--":
            continue
        if part.endswith(b"--"):
            part = part[:-2].rstrip()
        header_blob, separator, payload = part.partition(b"\r\n\r\n")
        if not separator:
            continue
        headers = header_blob.decode("utf-8", errors="ignore")
        disposition_match = re.search(
            r'content-disposition:.*name="' + re.escape(field_name) + r'"',
            headers,
            re.IGNORECASE,
        )
        if disposition_match:
            return payload.rstrip(b"\r\n")
    return None


def _multipart_first_file(body: bytes, content_type: str) -> bytes | None:
    match = re.search(r'boundary="?([^";]+)"?', content_type)
    if not match:
        return None
    boundary = b"--" + match.group(1).encode("utf-8")
    for raw_part in body.split(boundary):
        part = raw_part.strip()
        if not part or part == b"--":
            continue
        if part.endswith(b"--"):
            part = part[:-2].rstrip()
        header_blob, separator, payload = part.partition(b"\r\n\r\n")
        if not separator:
            continue
        headers = header_blob.decode("utf-8", errors="ignore")
        if "content-disposition:" not in headers.casefold():
            continue
        clean_payload = payload.rstrip(b"\r\n")
        if _image_type(clean_payload):
            return clean_payload
    return None


def _list_values(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        return [item.strip() for item in text.split(",") if item.strip()]
    return [value]


def _profile_id_from_headers(headers: Any) -> int | None:
    value = headers.get("x-rec-room-profile")
    profile_id = _int_value(value)
    if profile_id > 0:
        return profile_id

    authorization = str(headers.get("authorization") or "").strip()
    if authorization.casefold().startswith("bearer "):
        authorization = authorization[7:].strip()
    token_prefix = f"local-{API_VERSION}-"
    if authorization.casefold().startswith(token_prefix.casefold()):
        profile_id = _int_value(authorization[len(token_prefix) :])
        if profile_id > 0:
            return profile_id
    return None


def _profile_header(request: Request) -> int | None:
    return _profile_id_from_headers(request.headers)


def _load_state(context: Any, player_id: str) -> dict[str, Any]:
    with context.db.connection() as conn:
        row = conn.execute(
            "SELECT state_json FROM player_version_state WHERE player_id = ? AND api_version = ?",
            (player_id, API_VERSION),
        ).fetchone()
    if not row:
        return {}
    try:
        return json.loads(row["state_json"])
    except Exception:
        return {}


def _save_state(context: Any, player_id: str, state: dict[str, Any]) -> None:
    stamp = _now_iso()
    with context.db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO player_version_state (player_id, api_version, state_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(player_id, api_version) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (player_id, API_VERSION, json.dumps(state, sort_keys=True), stamp, stamp),
        )


def _row_by_recnet_id(context: Any, recnet_id: int) -> Any | None:
    with context.db.connection() as conn:
        return conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players p
            JOIN player_version_state pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND json_extract(pvs.state_json, '$.recnet_id') = ?
            """,
            (API_VERSION, recnet_id),
        ).fetchone()


def _row_by_platform(context: Any, platform: int, platform_id: str) -> Any | None:
    with context.db.connection() as conn:
        return conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players p
            JOIN player_version_state pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND json_extract(pvs.state_json, '$.platform') = ?
              AND json_extract(pvs.state_json, '$.platform_id') = ?
            """,
            (API_VERSION, platform, platform_id),
        ).fetchone()


def _allocated_recnet_id(context: Any, player_id: str, preferred: int) -> int:
    if preferred > 0:
        existing = _row_by_recnet_id(context, preferred)
        if not existing or existing["player_id"] == player_id:
            return preferred
    seed = abs(hash(player_id)) % 8_000_000_000
    candidate = 1_000_000_000 + seed
    while True:
        existing = _row_by_recnet_id(context, candidate)
        if not existing or existing["player_id"] == player_id:
            return candidate
        candidate += 1


def _seed_new_player_preferences(state: dict[str, Any]) -> bool:
    settings = state.get("settings")
    if not isinstance(settings, dict):
        settings = {}
        state["settings"] = settings
    changed = False
    if "Recroom.OOBE" not in settings:
        settings["Recroom.OOBE"] = "0"
        changed = True
    if "OOBE_OBJECTIVES_GRANTED" not in settings:
        settings["OOBE_OBJECTIVES_GRANTED"] = "0"
        changed = True
    if not state.get("new_player_preferences_seeded"):
        state["new_player_preferences_seeded"] = True
        changed = True
    return changed


def _seed_avatar_defaults(state: dict[str, Any]) -> bool:
    changed = False
    defaults = {
        "outfit_selections": _DEFAULT_OUTFIT_SELECTIONS,
        "skin_color": _DEFAULT_SKIN_COLOR,
        "hair_color": _DEFAULT_HAIR_COLOR,
    }
    for key, value in defaults.items():
        if key not in state or state[key] is None:
            state[key] = value
            changed = True
    return changed


def _profile_from_row(row: Any) -> dict[str, Any]:
    try:
        state = json.loads(row["state_json"])
    except Exception:
        state = {}
    return _profile_from_player(row, state)


def _developer_flag(player: Any) -> bool:
    if bool(player["is_coach"]):
        return True
    try:
        permissions_value = player["permissions"]
    except Exception:
        permissions_value = None
    if isinstance(permissions_value, list):
        return "DEV" in permissions_value
    try:
        permissions = json.loads(player["permissions_json"] or "[]")
    except Exception:
        permissions = []
    return "DEV" in permissions


def _profile_from_player(player: Any, state: dict[str, Any]) -> dict[str, Any]:
    recnet_id = int(state.get("recnet_id") or 0)
    platform = _int_value(state.get("platform"))
    platform_id = str(state.get("platform_id") or recnet_id)
    return {
        "Id": recnet_id,
        "Username": player["username"],
        "DisplayName": player["display_name"],
        "XP": int(player["canonical_xp"] or 0),
        "Level": int(player["canonical_level"] or 1),
        "RegistrationStatus": 2,
        "Developer": _developer_flag(player),
        "CanReceiveInvites": True,
        "ProfileImageName": str(state.get("profile_image_name") or ""),
        "JuniorProfile": False,
        "ForceJuniorImages": False,
        "PendingJunior": False,
        "HasBirthday": bool(state.get("has_birthday", True)),
        "AvoidJuniors": bool(state.get("avoid_juniors", False)),
        "PlayerReputation": {
            "Noteriety": int(state.get("noteriety") or 0),
            "CheerGeneral": int(state.get("cheer_general") or 0),
            "CheerHelpful": int(state.get("cheer_helpful") or 0),
            "CheerGreatHost": int(state.get("cheer_great_host") or 0),
            "CheerSportsman": int(state.get("cheer_sportsman") or 0),
            "CheerCreative": int(state.get("cheer_creative") or 0),
            "CheerCredit": int(state.get("cheer_credit") or 0),
            "SelectedCheer": None,
        },
        "PlatformId": {
            "Platform": platform,
            "PlatformId": platform_id,
        },
    }


def _ensure_player(
    context: Any,
    platform: int,
    platform_id: str,
    name: str | None,
) -> dict[str, Any]:
    identity_key = f"platform:{platform}:{platform_id}"
    fallback_name = f"Player{platform_id or 'Local'}"
    normal_name = None if _is_reserved_coach_name(name) else name
    username = _safe_username(normal_name, fallback_name)
    display_name = (str(normal_name).strip() if normal_name else username) or username
    player = context.get_or_create_player(
        API_VERSION,
        identity_key=identity_key,
        username=username,
        display_name=display_name,
    )
    if bool(player["is_coach"]):
        identity_key = f"{identity_key}:normal"
        username = _safe_username(None, fallback_name)
        player = context.get_or_create_player(
            API_VERSION,
            identity_key=identity_key,
            username=username,
            display_name=username,
        )
        if bool(player["is_coach"]):
            raise HTTPException(status_code=403, detail="Coach identity is reserved.")
    state = _load_state(context, player["player_id"])
    recnet_id = state.get("recnet_id")
    if not recnet_id:
        recnet_id = _allocated_recnet_id(context, player["player_id"], _int_value(platform_id))
    state.setdefault("identity_key", identity_key)
    state["recnet_id"] = int(recnet_id)
    state["platform"] = platform
    state["platform_id"] = str(platform_id)
    state.setdefault("login_token", f"local-{API_VERSION}-{recnet_id}")
    state.setdefault("analytics_session_id", _now_ticks())
    _seed_new_player_preferences(state)
    _seed_avatar_defaults(state)
    _save_state(context, player["player_id"], state)
    return _profile_from_player(player, state)


def _current_player(context: Any, request: Request) -> tuple[Any | None, dict[str, Any]]:
    recnet_id = _profile_header(request)
    if recnet_id is None:
        return None, {}
    return _state_for_recnet_id(context, recnet_id)


def _state_for_recnet_id(context: Any, recnet_id: int | None) -> tuple[Any | None, dict[str, Any]]:
    if recnet_id is None:
        return None, {}
    row = _row_by_recnet_id(context, recnet_id)
    if not row:
        return None, {}
    try:
        state = json.loads(row["state_json"])
    except Exception:
        state = {}
    return row, state


def _login_response(profile: dict[str, Any], context: Any) -> dict[str, Any]:
    row = _row_by_recnet_id(context, int(profile["Id"]))
    state = {}
    if row:
        try:
            state = json.loads(row["state_json"])
        except Exception:
            state = {}
    return {
        "Error": "",
        "Player": profile,
        "Token": str(state.get("login_token") or f"local-{API_VERSION}-{profile['Id']}"),
        "FirstLoginOfTheDay": True,
        "AnalyticsSessionId": int(state.get("analytics_session_id") or _now_ticks()),
    }


def _config_payload(context: Any) -> dict[str, Any]:
    return {
        "MessageOfTheDay": context.get_motd(API_VERSION),
        "CdnBaseUri": "",
        "MatchmakingParams": {
            "PreferFullRoomsFrequency": 0.7,
            "PreferEmptyRoomsFrequency": 0.3,
        },
        # 20180112 keeps a runtime allow-list of ActivityLevelId values.
        # Without this, the game session can deserialize yet still be rejected
        # with "RecNet game session contains unknown activity level ID".
        "ActivityLevelIds": sorted(_KNOWN_ACTIVITY_LEVEL_IDS),
        "LevelProgressionMaps": [
            {"Level": 1, "RequiredXp": 1000},
            {"Level": 2, "RequiredXp": 2000},
            {"Level": 3, "RequiredXp": 3000},
            {"Level": 4, "RequiredXp": 4000},
            {"Level": 5, "RequiredXp": 5000},
        ],
        "DailyObjectives": [
            [
                {"type": 100, "score": 1},
                {"type": 201, "score": 1},
                {"type": 300, "score": 1},
            ],
            [
                {"type": 300, "score": 1},
                {"type": 401, "score": 1},
                {"type": 500, "score": 1},
            ],
            [
                {"type": 500, "score": 1},
                {"type": 802, "score": 1},
                {"type": 200, "score": 1},
            ],
        ],
        "ConfigTable": [
            {"Key": "Gift.DropChance", "Value": "0"},
            {"Key": "Gift.XP", "Value": "0"},
        ],
        "PhotonConfig": {
            "CrcCheckEnabled": False,
            "EnableServerTracingAfterDisconnect": False,
        },
    }


async def _handle_platformlogin(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/platformlogin/v1/refreshlogin" and method == "GET":
        row, state = _current_player(context, request)
        token = state.get("login_token") if row else f"local-{API_VERSION}-anonymous"
        return JSONResponse({"Token": token})

    if path == "api/platformlogin/v1/getcachedlogins" and method == "POST":
        payload = await _json_body(request, {})
        platform = _int_value(payload.get("Platform") if isinstance(payload, dict) else None)
        platform_id = str(payload.get("PlatformId") if isinstance(payload, dict) else "")
        row = _row_by_platform(context, platform, platform_id)
        return JSONResponse([_profile_from_row(row)] if row else [])

    if path == "api/platformlogin/v1/removecachedlogin" and method == "POST":
        return Response(status_code=200)

    if path == "api/platformlogin/v1/registeraccount" and method == "POST":
        return JSONResponse({"Success": True, "Message": ""})

    if path in {"api/platformlogin/v1/logincached", "api/platformlogin/v1/createaccount"} and method == "POST":
        payload = await _json_body(request, {})
        if not isinstance(payload, dict):
            payload = {}
        platform = _int_value(payload.get("Platform"))
        platform_id = str(payload.get("PlatformId") or payload.get("PlayerId") or "")
        name = payload.get("Username") or payload.get("DisplayName") or payload.get("Email")
        if not name:
            name = f"Player{platform_id or 'Local'}"
        profile = _ensure_player(context, platform, platform_id, str(name))
        row, _ = _state_for_recnet_id(context, int(profile["Id"]))
        if row:
            context.remember_request_identities(row["player_id"], request, API_VERSION)
            context.record_player_identities(
                row["player_id"],
                [("account_id", f"platform:{platform}:{platform_id}")],
            )
        return JSONResponse(_login_response(profile, context))

    raise HTTPException(status_code=404, detail="Unknown platform login route")


async def _handle_settings(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()
    row, state = _current_player(context, request)

    if path in {"api/settings/v2", "api/settings/v2/"} and method == "GET":
        if row and _seed_new_player_preferences(state):
            _save_state(context, row["player_id"], state)
        settings = state.get("settings") if isinstance(state.get("settings"), dict) else {}
        if not settings:
            settings = {"Recroom.OOBE": "0", "OOBE_OBJECTIVES_GRANTED": "0"}
        return JSONResponse(
            [{"Key": str(key), "Value": str(value)} for key, value in settings.items()]
        )

    if path == "api/settings/v2/set" and method == "POST":
        payload = await _json_body(request, {})
        if row and isinstance(payload, dict):
            settings = state.get("settings") if isinstance(state.get("settings"), dict) else {}
            settings[str(payload.get("Key") or "")] = str(payload.get("Value") or "")
            state["settings"] = settings
            _save_state(context, row["player_id"], state)
        return Response(status_code=200)

    if path == "api/settings/v2/remove" and method == "POST":
        payload = await _json_body(request, {})
        if row and isinstance(payload, dict):
            settings = state.get("settings") if isinstance(state.get("settings"), dict) else {}
            settings.pop(str(payload.get("Key") or ""), None)
            state["settings"] = settings
            _save_state(context, row["player_id"], state)
        return Response(status_code=200)

    raise HTTPException(status_code=404, detail="Unknown settings route")


def _player_rows(context: Any) -> list[Any]:
    with context.db.connection() as conn:
        return conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players p
            JOIN player_version_state pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
            ORDER BY p.created_at ASC
            """,
            (API_VERSION,),
        ).fetchall()


def _player_ids_from_payload(payload: Any) -> list[Any]:
    if isinstance(payload, dict):
        raw_ids = (
            payload.get("PlayerIds")
            or payload.get("playerIds")
            or payload.get("Ids")
            or payload.get("ids")
            or payload.get("Players")
        )
    else:
        raw_ids = payload
    return _list_values(raw_ids)


def _update_player_display_name(context: Any, row: Any, state: dict[str, Any], name: str) -> dict[str, Any]:
    if _is_reserved_coach_name(name):
        raise HTTPException(status_code=403, detail="Coach identity is reserved.")
    clean_name = str(name or "").strip()
    if not clean_name:
        raise HTTPException(status_code=400, detail="Display name is required.")
    stamp = _now_iso()
    desired_username = _safe_username(clean_name, str(row["username"] or "Player"))
    with context.db.transaction() as conn:
        username_row = conn.execute(
            "SELECT player_id FROM players WHERE username = ? AND player_id <> ?",
            (desired_username, row["player_id"]),
        ).fetchone()
        username = row["username"] if username_row else desired_username
        conn.execute(
            """
            UPDATE players
            SET username = ?, display_name = ?, updated_at = ?
            WHERE player_id = ? AND is_coach = 0
            """,
            (username, clean_name[:64], stamp, row["player_id"]),
        )
    updated = _row_by_recnet_id(context, int(state.get("recnet_id") or 0))
    if updated:
        context.record_player_identities(
            updated["player_id"],
            [("username_lower", updated["username"]), ("username_lower", updated["display_name"])],
        )
    return _profile_from_row(updated) if updated else _profile_from_player(row, state)


async def _handle_players(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/players/v1/GetGeneratedNameOptions" and method == "GET":
        return JSONResponse(
            {
                "Nouns": ["Player", "Maker", "Explorer", "Builder"],
                "Adjectives": ["Brave", "Creative", "Friendly", "Lucky"],
            }
        )

    if path == "api/players/v1/list" and method == "POST":
        payload = await _json_body(request, [])
        profiles = []
        for value in _player_ids_from_payload(payload):
            row = _row_by_recnet_id(context, _int_value(value, -1))
            if row:
                profiles.append(_profile_from_row(row))
        return JSONResponse(profiles)

    if path == "api/players/v2/listByPlatformId" and method == "POST":
        payload = await _json_body(request, {})
        if not isinstance(payload, dict):
            payload = {}
        platform = _int_value(payload.get("Platform"), -1)
        profiles = []
        for platform_id in _list_values(payload.get("PlatformIds")):
            row = _row_by_platform(context, platform, str(platform_id))
            if row:
                profiles.append(_profile_from_row(row))
        return JSONResponse(profiles)

    match = re.fullmatch(r"api/players/v1/(\d+)", path)
    if match and method == "GET":
        row = _row_by_recnet_id(context, int(match.group(1)))
        if not row:
            raise HTTPException(status_code=404, detail="Player not found")
        return JSONResponse(_profile_from_row(row))

    if path == "api/players/v2/search" and method == "GET":
        needle = str(request.query_params.get("name") or "").strip().casefold()
        if not needle:
            return JSONResponse([])
        matches = []
        for row in _player_rows(context):
            profile = _profile_from_row(row)
            haystack = f"{profile.get('Username', '')} {profile.get('DisplayName', '')}".casefold()
            if needle in haystack:
                matches.append(profile)
        return JSONResponse(matches[:20])

    if path == "api/players/v1/phonelastfour" and method == "GET":
        row, state = _current_player(context, request)
        phone_number = str(state.get("phone_number") or "") if row else ""
        return JSONResponse({"PhoneNumber": phone_number[-4:] if len(phone_number) >= 4 else ""})

    if path == "api/players/v2/displayname" and method == "POST":
        row, state = _current_player(context, request)
        if not row:
            return _forbidden("Display-name changes require a logged-in player.")
        payload = await _json_body(request, {})
        name = payload.get("Name") if isinstance(payload, dict) else None
        _update_player_display_name(context, row, state, str(name or ""))
        return JSONResponse(_success_payload())

    if path == "api/players/v1/birthday" and method == "POST":
        row, state = _current_player(context, request)
        if not row:
            return _forbidden("Birthday updates require a logged-in player.")
        payload = await _json_body(request, {})
        if isinstance(payload, dict):
            state["birthday_date_string"] = str(payload.get("BirthdayDateString") or "")
        state["has_birthday"] = True
        state["is_junior"] = False
        _save_state(context, row["player_id"], state)
        return JSONResponse({"Success": True, "Message": "", "MustRestart": False, "IsJunior": False})

    if path == "api/players/v1/avoidJuniors" and method == "POST":
        row, state = _current_player(context, request)
        if not row:
            return _forbidden("Junior-avoidance preference requires a logged-in player.")
        payload = await _json_body(request, {})
        if isinstance(payload, dict):
            state["avoid_juniors"] = _bool_value(payload.get("AvoidJuniors"))
        _save_state(context, row["player_id"], state)
        return _empty_ok()

    if path == "api/players/v1/createProfile" and method == "POST":
        row, state = _current_player(context, request)
        if not row:
            return _forbidden("Profile creation requires a logged-in player.")
        payload = await _json_body(request, {})
        name = payload.get("Name") if isinstance(payload, dict) else None
        profile = _update_player_display_name(context, row, state, str(name or row["display_name"]))
        return JSONResponse(profile)

    if path == "api/players/v1/objectives" and method == "POST":
        row, state = _current_player(context, request)
        if not row:
            return _forbidden("Objective uploads require a logged-in player.")
        payload = await _json_body(request, [])
        state["last_objective_upload"] = payload if isinstance(payload, list) else [payload]
        state["last_objective_upload_at"] = _now_iso()
        _save_state(context, row["player_id"], state)
        return _empty_ok()

    if path == "api/players/v1/deleteProfile" and method == "POST":
        return _forbidden("Profile deletion is disabled for normal clients.")

    if path == "api/players/v2/updateReputation" and method == "POST":
        return _forbidden("Client-provided reputation changes require server authority.")

    if path in {"api/players/v2/phone", "api/players/v2/phone/verify"} and method == "POST":
        raise HTTPException(status_code=501, detail="Phone verification API confirmed but not implemented.")

    raise HTTPException(status_code=501, detail="Players API route confirmed but not implemented.")


def _success_payload(message: str = "") -> dict[str, Any]:
    return {"Success": True, "Message": message}


def _empty_ok() -> Response:
    return Response(status_code=200)


def _forbidden(message: str) -> JSONResponse:
    return JSONResponse(status_code=403, content={"detail": message})


def _avatar_payload(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "OutfitSelections": str(state.get("outfit_selections") if state.get("outfit_selections") is not None else _DEFAULT_OUTFIT_SELECTIONS),
        "SkinColor": str(state.get("skin_color") if state.get("skin_color") is not None else _DEFAULT_SKIN_COLOR),
        "HairColor": str(state.get("hair_color") if state.get("hair_color") is not None else _DEFAULT_HAIR_COLOR),
    }


def _saved_outfit_payload(raw: Any, slot: int = 0) -> dict[str, Any]:
    item = raw if isinstance(raw, dict) else {}
    return {
        "Slot": str(_int_value(item.get("Slot"), slot)),
        "PreviewImageName": str(item.get("PreviewImageName") or ""),
        "OutfitSelections": str(item.get("OutfitSelections") if item.get("OutfitSelections") is not None else _DEFAULT_OUTFIT_SELECTIONS),
        "SkinColor": str(item.get("SkinColor") if item.get("SkinColor") is not None else _DEFAULT_SKIN_COLOR),
        "HairColor": str(item.get("HairColor") if item.get("HairColor") is not None else _DEFAULT_HAIR_COLOR),
    }


def _avatar_item_payload(raw: Any) -> dict[str, Any]:
    item = raw if isinstance(raw, dict) else {}
    return {
        "AvatarItemDesc": str(item.get("AvatarItemDesc") or ""),
        "UnlockedLevel": _int_value(item.get("UnlockedLevel"), 0),
    }


def _state_list(state: dict[str, Any], key: str) -> list[Any]:
    value = state.get(key)
    return value if isinstance(value, list) else []


def _state_string_list(state: dict[str, Any], key: str) -> list[str]:
    values: list[str] = []
    for value in _state_list(state, key):
        text = str(value or "")
        if text and text not in values:
            values.append(text)
    return values


def _storefront_type_from_path(path: str, payload: Any = None) -> int:
    match = re.fullmatch(r"api/storefronts/v1/(?:balance/)?(\d+)", path)
    if match:
        return _int_value(match.group(1))
    if isinstance(payload, dict):
        return _int_value(payload.get("StorefrontType"))
    return 0


def _storefront_balance_payload(storefront_type: int, balance: int = 0) -> dict[str, Any]:
    return {"Balance": int(balance), "StorefrontType": int(storefront_type)}


def _storefront_update_payload(storefront_type: int, balance: int = 0) -> dict[str, Any]:
    payload = _storefront_balance_payload(storefront_type, balance)
    payload["BalanceUpdates"] = []
    return payload


def _storefront_catalog_payload() -> dict[str, Any]:
    return {
        "StoreItems": [],
        "StartAt": "2018-01-12T00:00:00Z",
        "EndAt": "2099-01-01T00:00:00Z",
    }


def _saved_image_names(context: Any, row: Any | None, state: dict[str, Any]) -> list[str]:
    names = _state_string_list(state, "saved_image_names")
    if not row:
        return names
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT asset_id
            FROM data_assets
            WHERE owner_player_id = ?
              AND purpose = ?
            ORDER BY created_at ASC
            """,
            (row["player_id"], f"{API_VERSION}.saved_image"),
        ).fetchall()
    for asset in rows:
        name = str(asset["asset_id"])
        if name not in names:
            names.append(name)
    return names


def _image_asset_row(context: Any, image_name: str) -> Any | None:
    if not image_name:
        return None
    with context.db.connection() as conn:
        return conn.execute(
            """
            SELECT asset_id, relative_path, mime_type
            FROM data_assets
            WHERE asset_id = ?
            """,
            (image_name,),
        ).fetchone()


def _stored_image_response(context: Any, image_name: str) -> Response:
    asset = _image_asset_row(context, image_name)
    if not asset:
        return Response(status_code=404)
    image_path = context.data_dir / asset["relative_path"]
    if not image_path.is_file():
        return Response(status_code=404)
    return Response(image_path.read_bytes(), media_type=asset["mime_type"])


async def _store_image_upload(
    path: str,
    request: Request,
    context: Any,
    *,
    purpose: str,
    state_key: str | None,
) -> str:
    row, state = _current_player(context, request)
    body = await _body_bytes(request)
    content_type = request.headers.get("content-type", "")
    image = _multipart_field(body, content_type, "image")
    if image is None:
        image = _multipart_first_file(body, content_type)
    if image is None and _image_type(body):
        image = body
    if not image:
        raise HTTPException(status_code=400, detail="Missing image upload.")

    detected = _image_type(image)
    if detected is None:
        raise HTTPException(status_code=400, detail="Unsupported image upload format.")
    file_ext, mime_type = detected
    asset = context.save_image_bytes(
        owner_player_id=row["player_id"] if row else None,
        content=image,
        file_ext=file_ext,
        mime_type=mime_type,
        purpose=f"{API_VERSION}.{purpose}",
        metadata={
            "route": path,
            "recnet_id": _profile_header(request),
            "gameSessionId": request.query_params.get("gameSessionId"),
            "oldImageName": request.query_params.get("oldImageName"),
        },
    )
    image_name = str(asset["asset_id"])
    if row:
        if state_key:
            names = _state_string_list(state, state_key)
            old_name = str(request.query_params.get("oldImageName") or "")
            if old_name:
                names = [name for name in names if name != old_name]
            if image_name not in names:
                names.append(image_name)
            state[state_key] = names
        if purpose == "profile_image":
            state["profile_image_name"] = image_name
            with context.db.transaction() as conn:
                conn.execute(
                    "UPDATE players SET profile_picture_asset_id = ?, updated_at = ? WHERE player_id = ?",
                    (asset["asset_id"], _now_iso(), row["player_id"]),
                )
        _save_state(context, row["player_id"], state)
    return image_name


async def _remove_image_reference(request: Request, context: Any, state_key: str, purpose: str) -> None:
    row, state = _current_player(context, request)
    payload = await _json_body(request, {})
    image_name = str(payload.get("ImageName") or "") if isinstance(payload, dict) else ""
    if row and image_name:
        state[state_key] = [name for name in _state_string_list(state, state_key) if name != image_name]
        with context.db.connection() as conn:
            asset = conn.execute(
                """
                SELECT relative_path
                FROM data_assets
                WHERE asset_id = ?
                  AND owner_player_id = ?
                  AND purpose = ?
                """,
                (image_name, row["player_id"], f"{API_VERSION}.{purpose}"),
            ).fetchone()
        if asset:
            image_path = (context.data_dir / asset["relative_path"]).resolve()
            data_dir = context.data_dir.resolve()
            if data_dir in image_path.parents and image_path.is_file():
                image_path.unlink()
            with context.db.transaction() as conn:
                conn.execute(
                    """
                    DELETE FROM data_assets
                    WHERE asset_id = ?
                      AND owner_player_id = ?
                      AND purpose = ?
                    """,
                    (image_name, row["player_id"], f"{API_VERSION}.{purpose}"),
                )
        _save_state(context, row["player_id"], state)


def _normalise_activity_level_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return _DORM_ACTIVITY_LEVEL_ID
    known = _KNOWN_ACTIVITY_LEVEL_IDS_CASEFOLD.get(text.casefold())
    if known:
        return known
    key = re.sub(r"[^a-z0-9]+", "", text.casefold())
    return _ACTIVITY_LEVEL_ALIASES.get(key, _DORM_ACTIVITY_LEVEL_ID)


def _activity_levels_from_payload(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return [_DORM_ACTIVITY_LEVEL_ID]
    for key in ("ActivityLevelIds", "activityLevelIds"):
        values = [
            _normalise_activity_level_id(item)
            for item in _list_values(payload.get(key))
            if item is not None
        ]
        values = [item for item in values if item in _KNOWN_ACTIVITY_LEVEL_IDS]
        if values:
            return values
    return [_activity_level_from_payload(payload)]


def _activity_level_from_payload(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("ActivityLevelId", "activityLevelId", "Activity", "activity", "Level", "level"):
            activity = payload.get(key)
            if activity is not None:
                return _normalise_activity_level_id(activity)
    return _DORM_ACTIVITY_LEVEL_ID


def _charades_words_payload() -> list[dict[str, Any]]:
    words: list[dict[str, Any]] = []
    for item in _CHARADES_WORDS:
        text = str(item.get("EN_US") or "").strip()
        if not text:
            continue
        words.append(
            {
                "EN_US": text,
                "Difficulty": _int_value(item.get("Difficulty"), 0),
            }
        )
    return words


def _game_session_id_for(player_id: int | None, activity_level_id: str) -> int:
    player_part = max(0, _int_value(player_id))
    level_part = int(activity_level_id[:8], 16)
    return ((player_part * 1_000_003) + level_part) % 2_147_483_647 or 1


def _game_session_room_id_for(player_id: int | None, activity_level_id: str) -> str:
    player_part = max(0, _int_value(player_id))
    return f"offline-{player_part}-{activity_level_id[:8]}"


def _game_session_payload(player_id: int | None = None, activity_level_id: str | None = None) -> dict[str, Any]:
    activity_level_id = _normalise_activity_level_id(activity_level_id)
    name = _ACTIVITY_LEVEL_DISPLAY_NAMES.get(activity_level_id, "Dorm Room")
    return {
        "GameSessionId": _game_session_id_for(player_id, activity_level_id),
        "RegionId": "offline",
        "RoomId": _game_session_room_id_for(player_id, activity_level_id),
        "EventId": None,
        "RecRoomId": None,
        "CreatorPlayerId": player_id,
        "Name": name,
        "ActivityLevelId": activity_level_id,
        "ActivityLevelIds": [activity_level_id],
        "Private": True,
        "Sandbox": False,
        "GameInProgress": False,
        "MaxCapacity": 1,
        "IsFull": False,
    }


def _game_session_response(
    request: Request, payload: Any = None, player_id: int | None = None
) -> dict[str, Any]:
    activity_level_id = _activity_levels_from_payload(payload)[0]
    if player_id is None:
        player_id = _profile_header(request)
    return {
        "Result": 0,
        "GameSession": _game_session_payload(
            player_id,
            activity_level_id,
        ),
    }


def _presence_ids_from_payload(payload: Any, fallback_player_id: int | None = None) -> list[int]:
    raw_ids: Any = payload
    if isinstance(payload, dict):
        for key in ("PlayerIds", "playerIds", "PlayerId", "playerId", "Ids", "ids"):
            if payload.get(key) is not None:
                raw_ids = payload.get(key)
                break
    ids: list[int] = []
    for item in _list_values(raw_ids):
        player_id = _int_value(item)
        if player_id > 0 and player_id not in ids:
            ids.append(player_id)
    if fallback_player_id and fallback_player_id > 0 and fallback_player_id not in ids:
        ids.append(fallback_player_id)
    return ids


def _game_session_player_id(request: Request, payload: Any) -> int | None:
    player_id = _profile_header(request)
    if player_id and player_id > 0:
        return player_id
    if not isinstance(payload, dict):
        return None
    for key in ("ExpectedPlayerIds", "expectedPlayerIds"):
        for item in _list_values(payload.get(key)):
            player_id = _int_value(item)
            if player_id > 0:
                return player_id
    return None


def _remember_presence_game_session(
    context: Any,
    row: Any | None,
    state: dict[str, Any],
    game_session: Any,
) -> bool:
    if not row or not isinstance(game_session, dict):
        return False
    state["presence_game_session"] = game_session
    known_sessions = state.get("known_game_sessions")
    if not isinstance(known_sessions, dict):
        known_sessions = {}
    session_id = _int_value(game_session.get("GameSessionId"))
    if session_id:
        known_sessions[str(session_id)] = game_session
    if len(known_sessions) > 20:
        known_sessions = dict(list(known_sessions.items())[-20:])
    state["known_game_sessions"] = known_sessions
    _save_state(context, row["player_id"], state)
    return True


def _offline_dorm_presence_session(player_id: int | None = None) -> dict[str, Any]:
    session = _game_session_payload(player_id, _DORM_ACTIVITY_LEVEL_ID)
    session["GameSessionId"] = -1
    session["RoomId"] = "offline"
    session["CreatorPlayerId"] = None
    session["IsFull"] = True
    return session


def _presence_payload(player_id: int, state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    game_session = state.get("presence_game_session")
    if not isinstance(game_session, dict):
        game_session = _offline_dorm_presence_session(player_id)
    return {
        "PlayerId": player_id,
        "IsOnline": True,
        "GameSession": game_session,
    }


def _room_id_for(state: dict[str, Any], player_id: int | None) -> int:
    room_id = _int_value(state.get("room_id"))
    if room_id > 0:
        return room_id
    if player_id and player_id > 0:
        return player_id * 100 + 1
    return 1


def _room_payload(state: dict[str, Any] | None = None, player_id: int | None = None) -> dict[str, Any]:
    state = state or {}
    stamp = str(state.get("room_created_at") or _now_iso())
    room_id = _room_id_for(state, player_id)
    creator_id = _int_value(state.get("room_creator_id"), player_id or 0)
    return {
        "RoomId": room_id,
        "Name": str(state.get("room_name") or "Dorm Room"),
        "Description": str(state.get("room_description") or ""),
        "CreatorPlayerId": creator_id,
        "DataBlobName": str(state.get("room_data_blob_name") or ""),
        "ActivityLevelId": _normalise_activity_level_id(state.get("room_activity_level_id")),
        "IsSandbox": bool(state.get("room_is_sandbox", False)),
        "MaxPlayers": _int_value(state.get("room_max_players"), 1),
        "FeaturedOrder": _int_value(state.get("room_featured_order"), 0),
        "Accessibility": _int_value(state.get("room_accessibility"), 0),
        "VisitorCount": _int_value(state.get("room_visitor_count"), 0),
        "CheerCount": _int_value(state.get("room_cheer_count"), 0),
        "ReportCount": _int_value(state.get("room_report_count"), 0),
        "State": _int_value(state.get("room_state"), 0),
        "StateModifiedAt": str(state.get("room_state_modified_at") or stamp),
        "CreatedAt": stamp,
        "ModifiedAt": str(state.get("room_modified_at") or stamp),
        "LastVisitedAt": str(state.get("room_last_visited_at") or stamp),
        "DataModifiedAt": state.get("room_data_modified_at"),
        "CoOwners": state.get("room_coowners") if isinstance(state.get("room_coowners"), list) else [],
        "Hosts": state.get("room_hosts") if isinstance(state.get("room_hosts"), list) else [],
        "PersonalDetails": {
            "IsCheering": bool(state.get("room_is_cheering", False)),
            "LastVisitedAt": str(state.get("room_last_visited_at") or stamp),
        },
    }


def _room_result(state: dict[str, Any] | None = None, player_id: int | None = None) -> dict[str, Any]:
    return {"Result": 0, "Room": _room_payload(state, player_id)}


def _room_row_payload(row: Any, player_id: int | None = None) -> dict[str, Any]:
    try:
        metadata = json.loads(row["metadata_json"] or "{}")
    except Exception:
        metadata = {}
    payload = _room_payload(metadata, player_id)
    payload["RoomId"] = _int_value(row["room_id"], payload["RoomId"])
    payload["Name"] = str(row["name"] or payload["Name"])
    payload["CreatorPlayerId"] = _int_value(metadata.get("room_creator_id"), payload["CreatorPlayerId"])
    payload["IsOfficial"] = bool(row["is_official"])
    return payload


def _room_row_by_id(context: Any, room_id: int) -> Any | None:
    if room_id <= 0:
        return None
    with context.db.connection() as conn:
        return conn.execute("SELECT * FROM rooms WHERE room_id = ?", (str(room_id),)).fetchone()


def _room_rows_for_owner(context: Any, owner_player_id: str) -> list[Any]:
    with context.db.connection() as conn:
        return conn.execute(
            """
            SELECT *
            FROM rooms
            WHERE owner_player_id = ?
            ORDER BY updated_at DESC
            """,
            (owner_player_id,),
        ).fetchall()


def _upsert_room_record(context: Any, row: Any, state: dict[str, Any], player_id: int | None) -> None:
    room_id = str(_room_id_for(state, player_id))
    stamp = _now_iso()
    metadata = dict(state)
    with context.db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO rooms(
                room_id, owner_player_id, creator_player_id, name, is_official,
                is_coach_only_edit, created_by_system, metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, 0, 0, 0, ?, ?, ?)
            ON CONFLICT(room_id) DO UPDATE SET
                name = excluded.name,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                room_id,
                row["player_id"],
                row["player_id"],
                str(state.get("room_name") or "Dorm Room"),
                json.dumps(metadata, sort_keys=True),
                str(state.get("room_created_at") or stamp),
                stamp,
            ),
        )


def _save_room_data_blob(
    context: Any,
    row: Any,
    room_id: int,
    content: bytes,
    image_names: list[str],
) -> str:
    blob_name = f"room-{room_id}-{int(time.time())}-{os.urandom(4).hex()}"
    stamp = _now_iso()
    with context.db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO room_data_blobs(
                blob_name, room_id, owner_player_id, data, image_list_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (blob_name, str(room_id), row["player_id"], content, json.dumps(image_names), stamp, stamp),
        )
    return blob_name


def _room_data_response(context: Any, blob_name: str) -> Response:
    with context.db.connection() as conn:
        row = conn.execute(
            "SELECT data FROM room_data_blobs WHERE blob_name = ?",
            (blob_name,),
        ).fetchone()
    if not row:
        return Response(status_code=404)
    return Response(bytes(row["data"]), media_type="application/octet-stream")


def _requested_room_id(path: str, payload: Any) -> int:
    if isinstance(payload, dict):
        room_id = _int_value(payload.get("RoomId"))
        if room_id > 0:
            return room_id
    match = re.search(r"/(\d+)$", path)
    return _int_value(match.group(1)) if match else 0


def _can_mutate_player_room(
    context: Any,
    row: Any,
    path: str,
    payload: Any,
    state: dict[str, Any],
    player_id: int | None,
) -> bool:
    if not player_id:
        return False
    requested_room_id = _requested_room_id(path, payload)
    if requested_room_id <= 0:
        requested_room_id = _room_id_for(state, player_id)
    room_row = _room_row_by_id(context, requested_room_id)
    if room_row:
        if bool(room_row["is_official"]) or bool(room_row["is_coach_only_edit"]):
            return bool(row["is_coach"])
        if room_row["owner_player_id"] != row["player_id"]:
            return bool(row["is_coach"])
    return requested_room_id == _room_id_for(state, player_id) or bool(row["is_coach"])


def _relationship_payload(player_id: int, *, relationship_type: int = 0, muted: int = 0, ignored: int = 0) -> dict[str, Any]:
    return {
        "PlayerID": player_id,
        "RelationshipType": relationship_type,
        "Muted": muted,
        "Ignored": ignored,
    }


async def _handle_relationships(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/relationships/v2/get" and method == "GET":
        return JSONResponse([])

    if path == "api/relationships/v1/bulkignoreplatformusers" and method == "POST":
        payload = await _json_body(request, {})
        if isinstance(payload, dict):
            state_key = "bulk_ignored_platform_users"
            row, state = _current_player(context, request)
            if row:
                state[state_key] = {
                    "Platform": _int_value(payload.get("Platform")),
                    "PlatformIds": list(payload.get("PlatformIds") or []),
                }
                _save_state(context, row["player_id"], state)
        return Response(status_code=200)

    match = re.fullmatch(r"api/relationships/v1/(mute|unmute|ignore|unignore)", path)
    if match and method == "POST":
        payload = await _json_body(request, {})
        player_id = _int_value(payload.get("PlayerId") if isinstance(payload, dict) else None)
        muted = 1 if match.group(1) == "mute" else 0
        ignored = 1 if match.group(1) == "ignore" else 0
        return JSONResponse(_relationship_payload(player_id, muted=muted, ignored=ignored))

    match = re.fullmatch(r"api/relationships/v2/(addfriend|removefriend|sendfriendrequest|acceptfriendrequest)", path)
    if match and method == "GET":
        relationship_types = {
            "addfriend": 3,
            "removefriend": 0,
            "sendfriendrequest": 1,
            "acceptfriendrequest": 3,
        }
        return JSONResponse(
            _relationship_payload(
                _int_value(request.query_params.get("id")),
                relationship_type=relationship_types[match.group(1)],
            )
        )

    raise HTTPException(status_code=501, detail="Relationships API route confirmed but not implemented.")


async def _handle_player_reporting(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/PlayerReporting/v1/moderationBlockDetails" and method == "GET":
        return JSONResponse(
            {
                "ReportCategory": 0,
                "Duration": 0,
                "GameSessionId": 0,
                "Message": "",
            }
        )

    if path in {"api/PlayerReporting/v1/voteToKick", "api/PlayerReporting/v1/kickFromEvent"} and method == "POST":
        return _forbidden("Player-facing kick and votekick actions are not allowed.")

    if path == "api/PlayerReporting/v2/create" and method == "POST":
        return JSONResponse(_success_payload())

    raise HTTPException(status_code=501, detail="PlayerReporting API route confirmed but not implemented.")


async def _handle_avatar(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()
    row, state = _current_player(context, request)

    if path == "api/avatar/v2" and method == "GET":
        if row and _seed_avatar_defaults(state):
            _save_state(context, row["player_id"], state)
        return JSONResponse(_avatar_payload(state))

    if path == "api/avatar/v2/set" and method == "POST":
        payload = await _json_body(request, {})
        if row and isinstance(payload, dict):
            state["outfit_selections"] = str(payload.get("OutfitSelections") or payload.get("outfitSelections") or "")
            state["skin_color"] = str(payload.get("SkinColor") or payload.get("skinColor") or "")
            state["hair_color"] = str(payload.get("HairColor") or payload.get("hairColor") or "")
            _save_state(context, row["player_id"], state)
        return _empty_ok()

    if path == "api/avatar/v1/saved" and method == "GET":
        return JSONResponse(
            [_saved_outfit_payload(item, index) for index, item in enumerate(_state_list(state, "saved_outfits"))]
        )

    if path == "api/avatar/v1/saved/set" and method == "POST":
        payload = await _json_body(request, {})
        if row:
            outfits = _state_list(state, "saved_outfits")
            if isinstance(payload, dict):
                key = _int_value(payload.get("Slot"), len(outfits))
                outfits = [item for item in outfits if not isinstance(item, dict) or _int_value(item.get("Slot")) != key]
                outfits.append(_saved_outfit_payload(payload, key))
                state["saved_outfits"] = outfits
                _save_state(context, row["player_id"], state)
        return _empty_ok()

    if path == "api/avatar/v3/items" and method == "GET":
        return JSONResponse([_avatar_item_payload(item) for item in _state_list(state, "avatar_items")])

    if path == "api/avatar/v2/gifts" and method == "GET":
        return JSONResponse([])

    if path in {"api/avatar/v1/gifts/requestDrop", "api/avatar/v2/gifts/generate"} and method == "POST":
        return JSONResponse(_success_payload())

    if path.startswith("api/avatar/v2/gifts/consume/") and method == "POST":
        return _empty_ok()

    raise HTTPException(status_code=501, detail="Avatar API route confirmed but not implemented.")


async def _handle_equipment(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()
    row, state = _current_player(context, request)

    if path == "api/equipment/v1/getUnlocked" and method == "GET":
        return JSONResponse(_state_list(state, "unlocked_equipment"))

    if path == "api/equipment/v1/update" and method == "POST":
        payload = await _json_body(request, [])
        if row:
            state["unlocked_equipment"] = payload if isinstance(payload, list) else [payload]
            _save_state(context, row["player_id"], state)
        return _empty_ok()

    raise HTTPException(status_code=501, detail="Equipment API route confirmed but not implemented.")


async def _handle_events(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/events/v3/list" and method == "GET":
        return JSONResponse([])

    if re.fullmatch(r"api/events/v1/status/\d+", path) and method == "GET":
        return JSONResponse({"Status": 0})

    raise HTTPException(status_code=501, detail="Events API route confirmed but not implemented.")


async def _handle_messages(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/messages/v2/get" and method == "GET":
        return JSONResponse([])

    if path in {"api/messages/v2/send", "api/messages/v2/delete", "api/messages/v1/sendMultiple"} and method == "POST":
        return _empty_ok()

    raise HTTPException(status_code=501, detail="Messages API route confirmed but not implemented.")


async def _handle_objectives(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/objectives/v1/myprogress" and method == "GET":
        return JSONResponse({"Objectives": [], "ObjectiveGroups": []})

    if path in {"api/objectives/v1/cleargroup", "api/objectives/v1/updateobjective", "api/objectives/v1/completegroup"} and method == "POST":
        return _empty_ok()

    raise HTTPException(status_code=501, detail="Objectives API route confirmed but not implemented.")


async def _handle_challenge(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/challenge/v1/getCurrent" and method == "GET":
        return JSONResponse({"Success": False, "Message": ""})

    if path == "api/challenge/v1/updateProgress" and method == "POST":
        return _empty_ok()

    raise HTTPException(status_code=501, detail="Challenge API route confirmed but not implemented.")


async def _handle_presence(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()
    player_id = _profile_header(request)
    row, state = _current_player(context, request)

    if path == "api/presence/v2/list" and method == "POST":
        payload = await _json_body(request, {})
        player_ids = _presence_ids_from_payload(payload, player_id)
        local_state = state if row else {}
        response_payload = [
            _presence_payload(current_id, local_state if current_id == player_id else None)
            for current_id in player_ids
        ]
        _trace_recnet(
            context,
            "presence_list_response",
            path=path,
            method=method,
            request_payload=payload,
            player_ids=player_ids,
            response_payload=response_payload,
        )
        return JSONResponse(response_payload)

    match = re.fullmatch(r"api/presence/v1/(\d+)", path)
    if match and method == "GET":
        requested_id = _int_value(match.group(1))
        return JSONResponse(_presence_payload(requested_id, state if requested_id == player_id else None))

    raise HTTPException(status_code=501, detail="Presence API route confirmed but not implemented.")


async def _handle_misc_success_or_empty(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/offlineinvite/v1/send" and method == "POST":
        return JSONResponse({"Message": ""})

    if path in {"api/PlayerCheer/v1/create", "api/PlayerCheer/v1/SetSelectedCheer"} and method == "POST":
        return JSONResponse(_success_payload())

    if path.startswith("api/storefronts/v1/balance") and method == "GET":
        return JSONResponse(_storefront_balance_payload(_storefront_type_from_path(path)))

    if path == "api/storefronts/v1/balance" and method == "POST":
        payload = await _json_body(request, {})
        return JSONResponse(_storefront_update_payload(_storefront_type_from_path(path, payload)))

    if re.fullmatch(r"api/storefronts/v1/\d+", path) and method == "GET":
        return JSONResponse(_storefront_catalog_payload())

    if path == "api/storefronts/v1/buy" and method == "POST":
        payload = await _json_body(request, {})
        return JSONResponse(_storefront_update_payload(_storefront_type_from_path(path, payload)))

    if path == "api/images/v1/listsaved" and method == "GET":
        row, state = _current_player(context, request)
        return JSONResponse({"Images": _saved_image_names(context, row, state)})

    if path in {"api/images/v2/uploadsaved", "api/images/v1/uploadsavedsingle"} and method == "POST":
        image_name = await _store_image_upload(
            path,
            request,
            context,
            purpose="saved_image",
            state_key="saved_image_names",
        )
        return JSONResponse({"ImageName": image_name})

    if path in {"api/images/v4/uploadtransient", "api/images/v1/uploadtransientsingle"} and method == "POST":
        image_name = await _store_image_upload(
            path,
            request,
            context,
            purpose="transient_image",
            state_key="transient_image_names",
        )
        return JSONResponse({"ImageName": image_name})

    if path == "api/images/v3/profile" and method == "POST":
        await _store_image_upload(
            path,
            request,
            context,
            purpose="profile_image",
            state_key=None,
        )
        return _empty_ok()

    if path == "api/images/v1/deletesaved" and method == "POST":
        await _remove_image_reference(request, context, "saved_image_names", "saved_image")
        return _empty_ok()

    if path == "api/images/v2/deletetransient" and method == "POST":
        await _remove_image_reference(request, context, "transient_image_names", "transient_image")
        return _empty_ok()

    if (
        path
        in {
            "api/images/v1/sendlink",
        }
        and method == "POST"
    ):
        return _empty_ok()

    if path.startswith("api/images/v1/named") and method == "GET":
        image_name = str(request.query_params.get("img") or "")
        if not image_name and path.startswith("api/images/v1/named/"):
            image_name = path.rsplit("/", 1)[-1]
        return _stored_image_response(context, image_name)

    raise HTTPException(status_code=501, detail="Route family confirmed but not implemented.")


async def _handle_simple_recovered_routes(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/Leaderboard/v1" and method == "POST":
        return JSONResponse(
            {
                "GlobalOverall": [],
                "GlobalPeriodic": [],
                "FriendsOverall": [],
                "FriendsPeriodic": [],
                "NextResetUTC": _now_iso(),
            }
        )

    if path == "api/playerReputation/v1/heal" and method == "POST":
        return _empty_ok()

    if path == "api/PlayerElo/v1/update" and method == "POST":
        return _empty_ok()

    if path == "api/PlayersBanned/v2/ban" and method == "POST":
        return _forbidden("Player ban requests require server authority.")

    if path == "api/upload/v1/crashdump" and method == "POST":
        return _empty_ok()

    raise HTTPException(status_code=501, detail="Recovered route family confirmed but not implemented.")


async def _handle_gamesessions(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if (
        path
        in {
            "api/gamesessions/v2/create",
            "api/gamesessions/v2/modify",
            "api/gamesessions/v2/join",
            "api/gamesessions/v2/joinevent",
            "api/gamesessions/v2/joinrandom",
            "api/gamesessions/v2/joinroom",
            "api/gamesessions/v2/joinplayer",
        }
        and method == "POST"
    ):
        payload = await _json_body(request, {})
        player_id = _game_session_player_id(request, payload)
        response_payload = _game_session_response(request, payload, player_id)
        row, state = _state_for_recnet_id(context, player_id)
        stored_presence = _remember_presence_game_session(
            context,
            row,
            state,
            response_payload.get("GameSession"),
        )
        _trace_recnet(
            context,
            "gamesession_response",
            path=path,
            method=method,
            request_payload=payload,
            player_id=player_id,
            stored_presence=stored_presence,
            response_payload=response_payload,
            selected_activity_level_id=response_payload.get("GameSession", {}).get("ActivityLevelId"),
        )
        return JSONResponse(response_payload)

    if path in {"api/gamesessions/v2/reportjoinresult", "api/gamesessions/v2/block"} and method == "POST":
        payload = await _json_body(request, {})
        player_id = _profile_header(request)
        row, state = _state_for_recnet_id(context, player_id)
        restored_presence = False
        if row and path == "api/gamesessions/v2/reportjoinresult" and isinstance(payload, dict):
            result = str(payload.get("Result") if payload.get("Result") is not None else "0").strip().casefold()
            if result in {"0", "success"}:
                session_id = str(_int_value(payload.get("GameSessionId")))
                known_sessions = state.get("known_game_sessions")
                if not isinstance(known_sessions, dict):
                    known_sessions = {}
                game_session = known_sessions.get(session_id)
                if not isinstance(game_session, dict):
                    game_session = state.get("presence_game_session")
                    if not isinstance(game_session, dict) or _int_value(
                        game_session.get("GameSessionId")
                    ) != _int_value(payload.get("GameSessionId")):
                        game_session = None
                if isinstance(game_session, dict):
                    if payload.get("RegionId") is not None:
                        game_session["RegionId"] = str(payload.get("RegionId") or "")
                    if payload.get("RoomId") is not None:
                        game_session["RoomId"] = str(payload.get("RoomId") or "")
                    restored_presence = _remember_presence_game_session(context, row, state, game_session)
        _trace_recnet(
            context,
            "gamesession_report",
            path=path,
            method=method,
            request_payload=payload,
            player_id=player_id,
            restored_presence=restored_presence,
        )
        return _empty_ok()

    raise HTTPException(status_code=501, detail="Game sessions API route confirmed but not implemented.")


async def _handle_rooms(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()
    row, state = _current_player(context, request)
    player_id = _profile_header(request)

    if path == "api/rooms/v1/myrooms" and method == "GET":
        if row:
            rooms = [_room_row_payload(room, player_id) for room in _room_rows_for_owner(context, row["player_id"])]
            if rooms:
                return JSONResponse(rooms)
        return JSONResponse([_room_payload(state, player_id)])

    if re.fullmatch(r"api/rooms/v1/details/\d+", path) and method == "GET":
        return JSONResponse({"PlayerCount": 1 if player_id else 0})

    if re.fullmatch(r"api/rooms/v1/\d+", path) and method == "GET":
        room_row = _room_row_by_id(context, _int_value(path.rsplit("/", 1)[-1]))
        if room_row:
            return JSONResponse(_room_row_payload(room_row, player_id))
        return JSONResponse(_room_payload(state, player_id))

    if path.startswith("api/rooms/v1/name/") and method == "GET":
        return JSONResponse(_room_payload(state, player_id))

    if path in {"api/rooms/v1/search", "api/rooms/v1/browse"} and method == "POST":
        return JSONResponse([_room_payload(state, player_id)])

    if path == "api/rooms/v2/create" and method == "POST":
        payload = await _json_body(request, {})
        if not row:
            return _forbidden("Room creation requires a logged-in player.")
        if row and isinstance(payload, dict):
            state["room_id"] = _room_id_for(state, player_id)
            state["room_creator_id"] = player_id or 0
            state["room_activity_level_id"] = _normalise_activity_level_id(payload.get("ActivityLevelId"))
            state["room_name"] = str(payload.get("Name") or "Dorm Room")
            state["room_description"] = str(payload.get("Description") or "")
            state["room_accessibility"] = _int_value(payload.get("Accessibility"), 0)
            state["room_is_sandbox"] = bool(payload.get("IsSandbox", False))
            state["room_max_players"] = _int_value(payload.get("MaxPlayers"), 1)
            state["room_created_at"] = state.get("room_created_at") or _now_iso()
            state["room_modified_at"] = _now_iso()
            _save_state(context, row["player_id"], state)
            _upsert_room_record(context, row, state, player_id)
        return JSONResponse(_room_result(state, player_id))

    if path.startswith("api/rooms/v1/modify/") and method == "POST":
        payload = await _json_body(request, {})
        if not row or not _can_mutate_player_room(context, row, path, payload, state, player_id):
            return _forbidden("You can only edit rooms owned by the current player.")
        if row and isinstance(payload, dict):
            if "Name" in payload:
                state["room_name"] = str(payload.get("Name") or "Dorm Room")
            if "Description" in payload:
                state["room_description"] = str(payload.get("Description") or "")
            if "Accessibility" in payload:
                state["room_accessibility"] = _int_value(payload.get("Accessibility"), 0)
            if "MaxPlayers" in payload:
                state["room_max_players"] = _int_value(payload.get("MaxPlayers"), 1)
            state["room_id"] = _room_id_for(state, player_id)
            state["room_modified_at"] = _now_iso()
            _save_state(context, row["player_id"], state)
            _upsert_room_record(context, row, state, player_id)
        return JSONResponse(_room_result(state, player_id))

    if path in {
        "api/rooms/v1/addcoowner",
        "api/rooms/v1/removecoowner",
        "api/rooms/v1/addhost",
        "api/rooms/v1/removehost",
    } and method == "POST":
        payload = await _json_body(request, {})
        if not row or not _can_mutate_player_room(context, row, path, payload, state, player_id):
            return _forbidden("You can only edit permissions on rooms owned by the current player.")
        target_id = _int_value(payload.get("PlayerId") if isinstance(payload, dict) else None)
        if row and target_id > 0:
            list_key = "room_coowners" if "coowner" in path else "room_hosts"
            values = state.get(list_key) if isinstance(state.get(list_key), list) else []
            if path.endswith(("addcoowner", "addhost")) and target_id not in values:
                values.append(target_id)
            if path.endswith(("removecoowner", "removehost")):
                values = [value for value in values if _int_value(value) != target_id]
            state[list_key] = values
            state["room_modified_at"] = _now_iso()
            _save_state(context, row["player_id"], state)
            _upsert_room_record(context, row, state, player_id)
        return JSONResponse({"Result": 0})

    if path == "api/rooms/v1/saveData" or re.fullmatch(r"api/rooms/v1/saveData/\d+", path):
        if method == "POST":
            body = await _body_bytes(request)
            content_type = request.headers.get("content-type", "")
            room_id = _requested_room_id(path, {})
            if room_id <= 0:
                room_id = _room_id_for(state, player_id)
            if not row or not _can_mutate_player_room(context, row, path, {"RoomId": room_id}, state, player_id):
                return _forbidden("You can only save data for rooms owned by the current player.")
            if row:
                data = _multipart_field(body, content_type, "data")
                if data is None:
                    data = body
                image_names: list[str] = []
                img_list_raw = _multipart_field(body, content_type, "imgList")
                if img_list_raw:
                    try:
                        parsed = json.loads(img_list_raw.decode("utf-8"))
                        image_names = [
                            str(item)
                            for item in _list_values(parsed.get("roomImageList") if isinstance(parsed, dict) else parsed)
                            if str(item)
                        ]
                    except Exception:
                        image_names = []
                blob_name = _save_room_data_blob(context, row, room_id, data or b"", image_names)
                state["room_id"] = room_id
                state["room_data_blob_name"] = blob_name
                state["room_data_modified_at"] = _now_iso()
                _save_state(context, row["player_id"], state)
                _upsert_room_record(context, row, state, player_id)
            return JSONResponse({"DataBlobName": str(state.get("room_data_blob_name") or "")})

    if path == "api/rooms/v1/cheer" and method == "POST":
        payload = await _json_body(request, {})
        is_cheering = bool(payload.get("Cheer", True)) if isinstance(payload, dict) else True
        if row:
            state["room_is_cheering"] = is_cheering
            state["room_cheer_count"] = 1 if is_cheering else 0
            _save_state(context, row["player_id"], state)
        return JSONResponse({"Success": True, "Message": ""})

    if path == "api/rooms/v1/report" and method == "POST":
        if row:
            state["room_report_count"] = _int_value(state.get("room_report_count"), 0) + 1
            _save_state(context, row["player_id"], state)
        return JSONResponse(_success_payload())

    raise HTTPException(status_code=501, detail="Rooms API route confirmed but not implemented.")


async def handle_http(route_path: str, request: Request, context: Any) -> Response:
    path = route_path.strip("/")
    method = request.method.upper()

    if path == "api/versioncheck/v3" and method == "GET":
        return Response(status_code=200)

    if path == "api/config/v2" and method == "GET":
        payload = _config_payload(context)
        _trace_recnet(
            context,
            "config_response",
            path=path,
            method=method,
            activity_level_ids=payload.get("ActivityLevelIds"),
        )
        return JSONResponse(payload)

    if path == "api/config/v1/amplitude" and method == "GET":
        return JSONResponse({"AmplitudeKey": _DEFAULT_AMPLITUDE_KEY})

    if "charades" in path.casefold() and "word" in path.casefold() and method == "GET":
        return JSONResponse(_charades_words_payload())

    if path.startswith("room/") and method == "GET":
        return _room_data_response(context, path.split("/", 1)[1])

    if path.startswith("api/platformlogin/"):
        return await _handle_platformlogin(path, request, context)

    if path.startswith("api/settings/"):
        return await _handle_settings(path, request, context)

    if path.startswith("api/players/"):
        return await _handle_players(path, request, context)

    if path.startswith("api/relationships/"):
        return await _handle_relationships(path, request, context)

    if path.startswith("api/PlayerReporting/"):
        return await _handle_player_reporting(path, request, context)

    if path.startswith("api/avatar/"):
        return await _handle_avatar(path, request, context)

    if path.startswith("api/equipment/"):
        return await _handle_equipment(path, request, context)

    if path.startswith("api/events/"):
        return await _handle_events(path, request, context)

    if path.startswith("api/messages/"):
        return await _handle_messages(path, request, context)

    if path.startswith("api/objectives/"):
        return await _handle_objectives(path, request, context)

    if path.startswith("api/challenge/"):
        return await _handle_challenge(path, request, context)

    if path.startswith("api/presence/"):
        return await _handle_presence(path, request, context)

    if path.startswith("api/gamesessions/"):
        return await _handle_gamesessions(path, request, context)

    if path.startswith("api/rooms/"):
        return await _handle_rooms(path, request, context)

    if path.startswith(("api/offlineinvite/", "api/PlayerCheer/", "api/storefronts/", "api/images/")):
        return await _handle_misc_success_or_empty(path, request, context)

    if path.startswith(
        (
            "api/Leaderboard/",
            "api/playerReputation/",
            "api/PlayerElo/",
            "api/PlayersBanned/",
            "api/upload/",
        )
    ):
        return await _handle_simple_recovered_routes(path, request, context)

    for prefix in _KNOWN_UNIMPLEMENTED_PREFIXES:
        if path.startswith(prefix):
            raise HTTPException(
                status_code=501,
                detail=f"{prefix.rstrip('/')} API confirmed in the 2018 client but not implemented.",
            )

    raise HTTPException(status_code=404, detail="Unknown route")


async def handle_websocket(route_path: str, websocket: WebSocket, context: Any) -> None:
    path = route_path.strip("/")
    if path != "api/notification/v2":
        await websocket.close(code=1008)
        return

    await websocket.accept()
    player_id = _profile_id_from_headers(websocket.headers)

    async def send_presence_heartbeat() -> None:
        if not player_id:
            return
        row, state = _state_for_recnet_id(context, player_id)
        presence = _presence_payload(player_id, state if row else {})
        _trace_recnet(
            context,
            "presence_heartbeat_response",
            player_id=player_id,
            has_state=bool(row),
            game_session=presence.get("GameSession"),
        )
        await websocket.send_text(
            json.dumps(
                {
                    "Id": 4,
                    "Msg": presence,
                }
            )
        )

    try:
        await websocket.send_text(json.dumps({"SessionId": _now_ticks()}))
        while True:
            message = await websocket.receive_text()
            command = message.strip().casefold()
            parsed = None
            try:
                parsed = json.loads(message)
            except Exception:
                parsed = None
            if isinstance(parsed, dict):
                if parsed.get("PlayerId") is not None:
                    player_id = _int_value(parsed.get("PlayerId")) or player_id
                command = str(parsed.get("api") or parsed.get("type") or parsed.get("Type") or "").strip().casefold()
            if command in {"ping", "heartbeat"}:
                await send_presence_heartbeat()
    except WebSocketDisconnect:
        return
