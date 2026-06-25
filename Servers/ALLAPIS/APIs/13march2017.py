"""13 March 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from the game builds at manifests 1917087114031565919 and
7031178245801109076:
- RecNet still uses COGCNMJCNKN.
- HTTP/WebSocket URL fields remain EHBCBOGDLDB and FPGKGDJLOJJ.
- Login posts the same real /api/platformlogin/v1 form as 9 March.
- Player subscription synchronization remains notification-WebSocket driven;
  REST api/PlayerSubscriptions/v1/init/add/remove is not a real route here.
- Messages include a v1 sendMultiple call, and offline game invites post to
  api/offlineinvite/v1/send.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response

API_VERSION = "13march2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Mon, 13 Mar 2017 19:47:14 GMT"


def _retarget_module(module) -> None:
    module.API_VERSION = API_VERSION
    module.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    if hasattr(module, "_BASE"):
        module._BASE.API_VERSION = API_VERSION
        module._BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
        module._BASE._set_api_version(module._BASE)
    if hasattr(module, "_PLATFORM_BASE"):
        module._PLATFORM_BASE.API_VERSION = API_VERSION
    if hasattr(module, "_SHARED"):
        _retarget_module(module._SHARED)


def _load_shared_adapter():
    module_path = Path(__file__).with_name("9march2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_9march2017_shared_for_13march2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 9march2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()
_BASE = _SHARED._BASE
_PLATFORM_BASE = _SHARED._PLATFORM_BASE


def _ensure_local_profile(request: Request, context) -> int:
    player_id = _SHARED._local_profile_id(request)
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    return player_id


def _safe_display_name(name: str, fallback: str) -> str:
    cleaner = getattr(_PLATFORM_BASE, "_safe_display_name", None)
    if callable(cleaner):
        return cleaner(name, fallback=fallback)
    name = str(name or "").strip()
    return name[:64] if name else fallback


async def _handle_display_name_update(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")

    payload = await _BASE._parse_client_payload(request)
    raw_name = _BASE._str_field(payload, "Name", "name", "DisplayName", "displayName")
    if not raw_name:
        return JSONResponse({"Success": False, "Message": "Name is required."})

    state = dict(player.get("state") or {})
    previous_name = str(state.get("name") or player.get("display_name") or player.get("username") or "Player")
    display_name = _safe_display_name(raw_name, fallback=previous_name)
    state["name"] = display_name

    api_state = context.ensure_player_version_state(player["player_id"], API_VERSION, {})
    api_state["name"] = display_name

    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET display_name = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND is_coach = 0
            """,
            (display_name, player["player_id"]),
        )
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], _PLATFORM_BASE.STATE_API_VERSION),
        )
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND api_version = ?
            """,
            (json.dumps(api_state, sort_keys=True), player["player_id"], API_VERSION),
        )
    context.record_player_identities(player["player_id"], [("username_lower", display_name)])
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_profile_image_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return await _BASE._handle_set_profile_image(request, f"api/images/v1/profile/{player_id}", context)


def _coerce_player_ids(values: Any) -> list[int]:
    if not isinstance(values, list):
        raise HTTPException(status_code=400, detail="ToPlayerIds must be a list.")

    player_ids: list[int] = []
    for value in values:
        try:
            player_id = int(value)
        except Exception:
            continue
        if player_id > 0:
            player_ids.append(player_id)
    return player_ids


def _append_message(context, *, from_id: int, to_id: int, message_type: int, data: str) -> dict[str, Any]:
    _BASE._ensure_existing_profile(context, to_id)
    messages = _BASE._all_messages(context)
    next_id = max([int(message.get("Id") or 0) for message in messages if isinstance(message, dict)] + [0]) + 1
    message = {
        "Id": next_id,
        "FromPlayerId": from_id,
        "ToPlayerId": to_id,
        "SentTime": _BASE._dotnet_utc_ticks(),
        "Type": int(message_type),
        "Data": str(data or ""),
    }
    messages.append(message)
    _BASE._save_messages(context, messages)
    return message


async def _handle_send_multiple_messages(request: Request, context) -> Response:
    from_id = _ensure_local_profile(request, context)
    payload = await _BASE._parse_client_payload(request)
    player_ids = _coerce_player_ids(payload.get("ToPlayerIds") or payload.get("toPlayerIds") or [])
    message_type = _BASE._int_field(payload, "Type", "type", default=0)
    data = _BASE._str_field(payload, "Data", "data")
    for to_id in player_ids:
        _append_message(context, from_id=from_id, to_id=to_id, message_type=message_type, data=data)
    return Response(status_code=204)


async def _handle_offline_invite(request: Request, context) -> Response:
    from_id = _ensure_local_profile(request, context)
    payload = await _BASE._parse_client_payload(request)
    to_id = _BASE._int_field(payload, "PlayerId", "playerId", default=0)
    if to_id <= 0:
        raise HTTPException(status_code=400, detail="PlayerId is required.")
    _append_message(context, from_id=from_id, to_id=to_id, message_type=0, data="")
    return JSONResponse({"Message": ""})


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _SHARED._clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path in {"api/messages/v1/sendmultiple", "api/messages/v1/sendmultiple/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Message sendMultiple method is not implemented.")
        return await _handle_send_multiple_messages(request, context)

    if path in {"api/offlineinvite/v1/send", "api/offlineinvite/v1/send/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Offline invite method is not implemented.")
        return await _handle_offline_invite(request, context)

    if path in {"api/players/v2/displayname", "api/players/v2/displayname/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Display name method is not implemented.")
        return await _handle_display_name_update(request, context)

    if path in {"api/images/v2/profile", "api/images/v2/profile/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Profile image upload method is not implemented.")
        return await _handle_profile_image_v2(request, context)

    return await _SHARED.handle_http(request=request, route_path=route_path, context=context)

handle_websocket = _SHARED.handle_websocket
