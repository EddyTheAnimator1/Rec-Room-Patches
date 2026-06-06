"""RecNet HTTP surface for the 12 January 2018 build.

Confirmed from first-party non-Photon client code in Assembly-CSharp.dll.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs

from fastapi import HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response


API_VERSION = "12january2018"
_DEFAULT_AMPLITUDE_KEY = "f1779b982f1c09aed3adb3cca563cbc2"

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


def _profile_header(request: Request) -> int | None:
    value = request.headers.get("x-rec-room-profile")
    if not value:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _load_state(context: Any, player_id: str) -> dict[str, Any]:
    with context.db.connect() as conn:
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
    with context.db.connect() as conn:
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
    with context.db.connect() as conn:
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
    _save_state(context, player["player_id"], state)
    return _profile_from_player(player, state)


def _current_player(context: Any, request: Request) -> tuple[Any | None, dict[str, Any]]:
    recnet_id = _profile_header(request)
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
    with context.db.connect() as conn:
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
    with context.db.transaction() as conn:
        conn.execute(
            "UPDATE players SET display_name = ?, updated_at = ? WHERE player_id = ? AND is_coach = 0",
            (clean_name[:64], stamp, row["player_id"]),
        )
    updated = _row_by_recnet_id(context, int(state.get("recnet_id") or 0))
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
        "Id": int(state.get("recnet_id") or 0),
        "AvatarItemDesc": str(state.get("avatar_item_desc") or ""),
        "OutfitSelections": str(state.get("outfit_selections") or ""),
    }


def _state_list(state: dict[str, Any], key: str) -> list[Any]:
    value = state.get(key)
    return value if isinstance(value, list) else []


def _game_session_payload(player_id: int | None = None) -> dict[str, Any]:
    return {
        "GameSessionId": 1,
        "RegionId": "offline",
        "RoomId": "offline",
        "EventId": None,
        "RecRoomId": None,
        "CreatorPlayerId": player_id,
        "Name": "Dorm Room",
        "ActivityLevelId": "DORM_ROOM",
        "Private": True,
        "Sandbox": False,
        "GameInProgress": False,
        "MaxCapacity": 1,
        "IsFull": False,
    }


def _game_session_response(request: Request) -> dict[str, Any]:
    return {
        "Result": 0,
        "GameSession": _game_session_payload(_profile_header(request)),
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
        "ActivityLevelId": str(state.get("room_activity_level_id") or "DORM_ROOM"),
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


def _requested_room_id(path: str, payload: Any) -> int:
    if isinstance(payload, dict):
        room_id = _int_value(payload.get("RoomId"))
        if room_id > 0:
            return room_id
    match = re.search(r"/(\d+)$", path)
    return _int_value(match.group(1)) if match else 0


def _can_mutate_player_room(path: str, payload: Any, state: dict[str, Any], player_id: int | None) -> bool:
    if not player_id:
        return False
    requested_room_id = _requested_room_id(path, payload)
    return requested_room_id <= 0 or requested_room_id == _room_id_for(state, player_id)


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
        return JSONResponse(_avatar_payload(state))

    if path == "api/avatar/v2/set" and method == "POST":
        payload = await _json_body(request, {})
        if row and isinstance(payload, dict):
            state["avatar_item_desc"] = str(payload.get("AvatarItemDesc") or payload.get("avatarItemDesc") or "")
            state["outfit_selections"] = str(payload.get("OutfitSelections") or payload.get("outfitSelections") or "")
            _save_state(context, row["player_id"], state)
        return _empty_ok()

    if path == "api/avatar/v1/saved" and method == "GET":
        return JSONResponse(_state_list(state, "saved_outfits"))

    if path == "api/avatar/v1/saved/set" and method == "POST":
        payload = await _json_body(request, {})
        if row:
            outfits = _state_list(state, "saved_outfits")
            if isinstance(payload, dict):
                key = payload.get("Slot") or payload.get("Name") or payload.get("OutfitSlot") or len(outfits)
                outfits = [item for item in outfits if item.get("Slot") != key] if all(isinstance(item, dict) for item in outfits) else []
                payload["Slot"] = key
                outfits.append(payload)
                state["saved_outfits"] = outfits
                _save_state(context, row["player_id"], state)
        return _empty_ok()

    if path in {"api/avatar/v2/gifts", "api/avatar/v3/items"} and method == "GET":
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
        return JSONResponse([])

    if path in {"api/objectives/v1/cleargroup", "api/objectives/v1/updateobjective", "api/objectives/v1/completegroup"} and method == "POST":
        return _empty_ok()

    raise HTTPException(status_code=501, detail="Objectives API route confirmed but not implemented.")


async def _handle_challenge(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/challenge/v1/getCurrent" and method == "GET":
        return JSONResponse([])

    if path == "api/challenge/v1/updateProgress" and method == "POST":
        return _empty_ok()

    raise HTTPException(status_code=501, detail="Challenge API route confirmed but not implemented.")


async def _handle_presence(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/presence/v2/list" and method == "POST":
        return JSONResponse([])

    match = re.fullmatch(r"api/presence/v1/(\d+)", path)
    if match and method == "GET":
        return JSONResponse({"PlayerId": _int_value(match.group(1)), "Status": 0})

    raise HTTPException(status_code=501, detail="Presence API route confirmed but not implemented.")


async def _handle_misc_success_or_empty(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/offlineinvite/v1/send" and method == "POST":
        return JSONResponse({"Message": ""})

    if path in {"api/PlayerCheer/v1/create", "api/PlayerCheer/v1/SetSelectedCheer"} and method == "POST":
        return JSONResponse(_success_payload())

    if path.startswith("api/storefronts/v1/balance") and method == "GET":
        return JSONResponse({"Balance": 0})

    if re.fullmatch(r"api/storefronts/v1/\d+", path) and method == "GET":
        return JSONResponse([])

    if path == "api/storefronts/v1/buy" and method == "POST":
        return JSONResponse(_success_payload())

    if path == "api/images/v1/listsaved" and method == "GET":
        return JSONResponse([])

    if (
        path
        in {
            "api/images/v1/deletesaved",
            "api/images/v2/deletetransient",
            "api/images/v2/uploadsaved",
            "api/images/v1/uploadsavedsingle",
            "api/images/v1/sendlink",
        }
        and method == "POST"
    ):
        return JSONResponse(_success_payload())

    if path.startswith("api/images/v1/named") and method == "GET":
        return Response(status_code=404)

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
        return JSONResponse(_game_session_response(request))

    if path in {"api/gamesessions/v2/reportjoinresult", "api/gamesessions/v2/block"} and method == "POST":
        return _empty_ok()

    raise HTTPException(status_code=501, detail="Game sessions API route confirmed but not implemented.")


async def _handle_rooms(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()
    row, state = _current_player(context, request)
    player_id = _profile_header(request)

    if path == "api/rooms/v1/myrooms" and method == "GET":
        return JSONResponse([_room_payload(state, player_id)])

    if re.fullmatch(r"api/rooms/v1/details/\d+", path) and method == "GET":
        return JSONResponse({"PlayerCount": 1 if player_id else 0})

    if re.fullmatch(r"api/rooms/v1/\d+", path) and method == "GET":
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
            state["room_activity_level_id"] = str(payload.get("ActivityLevelId") or "DORM_ROOM")
            state["room_name"] = str(payload.get("Name") or "Dorm Room")
            state["room_description"] = str(payload.get("Description") or "")
            state["room_accessibility"] = _int_value(payload.get("Accessibility"), 0)
            state["room_is_sandbox"] = bool(payload.get("IsSandbox", False))
            state["room_max_players"] = _int_value(payload.get("MaxPlayers"), 1)
            state["room_created_at"] = state.get("room_created_at") or _now_iso()
            state["room_modified_at"] = _now_iso()
            _save_state(context, row["player_id"], state)
        return JSONResponse(_room_result(state, player_id))

    if path.startswith("api/rooms/v1/modify/") and method == "POST":
        payload = await _json_body(request, {})
        if not row or not _can_mutate_player_room(path, payload, state, player_id):
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
        return JSONResponse(_room_result(state, player_id))

    if path in {
        "api/rooms/v1/addcoowner",
        "api/rooms/v1/removecoowner",
        "api/rooms/v1/addhost",
        "api/rooms/v1/removehost",
    } and method == "POST":
        payload = await _json_body(request, {})
        if not row or not _can_mutate_player_room(path, payload, state, player_id):
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
        return JSONResponse({"Result": 0})

    if path == "api/rooms/v1/saveData" or re.fullmatch(r"api/rooms/v1/saveData/\d+", path):
        if method == "POST":
            if not row or not _can_mutate_player_room(path, {}, state, player_id):
                return _forbidden("You can only save data for rooms owned by the current player.")
            if row:
                state["room_data_modified_at"] = _now_iso()
                _save_state(context, row["player_id"], state)
            return JSONResponse({"DataBlobName": ""})

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
        return JSONResponse(_config_payload(context))

    if path == "api/config/v1/amplitude" and method == "GET":
        return JSONResponse({"AmplitudeKey": _DEFAULT_AMPLITUDE_KEY})

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
    try:
        await websocket.receive_text()
        await websocket.send_text(json.dumps({"SessionId": _now_ticks()}))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        return
