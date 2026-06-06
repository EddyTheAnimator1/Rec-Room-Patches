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

_KNOWN_UNIMPLEMENTED_PREFIXES = (
    "api/avatar/",
    "api/challenge/",
    "api/equipment/",
    "api/events/",
    "api/gamesessions/",
    "api/images/",
    "api/Leaderboard/",
    "api/messages/",
    "api/objectives/",
    "api/offlineinvite/",
    "api/playerReputation/",
    "api/PlayerCheer/",
    "api/PlayerElo/",
    "api/PlayerReporting/",
    "api/PlayersBanned/",
    "api/presence/",
    "api/relationships/",
    "api/rooms/",
    "api/storefronts/",
    "api/upload/",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _now_ticks() -> int:
    return int(time.time() * 10_000_000) + 621355968000000000


def _safe_username(value: str | None, fallback: str) -> str:
    value = (value or "").strip() or fallback
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^A-Za-z0-9_.-]", "", value)
    return value[:32] or fallback


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
        "Developer": bool(player["is_coach"]),
        "CanReceiveInvites": True,
        "ProfileImageName": str(state.get("profile_image_name") or ""),
        "JuniorProfile": False,
        "ForceJuniorImages": False,
        "PendingJunior": False,
        "HasBirthday": True,
        "AvoidJuniors": False,
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
    username = _safe_username(name, f"Player{platform_id or 'Local'}")
    player = context.get_or_create_player(
        API_VERSION,
        identity_key=identity_key,
        username=username,
        display_name=(name or username),
    )
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
