"""9 March 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 512603081605663477:
- The late-February objective and notification-WebSocket family remains active.
- New player routes appear for current profile, list-by-platform-id, search,
  display-name updates, and phone verification.
- Messages add api/messages/v1/sendMultiple and offline invites add
  api/offlineinvite/v1/send.
- Player subscription synchronization remains notification-WebSocket driven
  rather than the older REST api/PlayerSubscriptions/v1/init/add/remove surface.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response

API_VERSION = "9march2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Thu, 09 Mar 2017 02:36:59 GMT"


def _retarget_module(module) -> None:
    module.API_VERSION = API_VERSION
    module.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    if hasattr(module, "_BASE"):
        module._BASE.API_VERSION = API_VERSION
        module._BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
        module._BASE._set_api_version(module._BASE)
    if hasattr(module, "_SHARED"):
        _retarget_module(module._SHARED)


def _load_shared_adapter():
    module_path = Path(__file__).with_name("1march2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_1march2017_shared_for_9march2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 1march2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


def _find_attr(module, attr: str):
    current = module
    while current is not None:
        if hasattr(current, attr):
            return getattr(current, attr)
        current = getattr(current, "_SHARED", None)
    raise RuntimeError(f"Shared adapter does not expose {attr}.")


_SHARED = _load_shared_adapter()
_FEB22 = _find_attr(_SHARED, "_SHARED")
_BASE = _find_attr(_SHARED, "_BASE")
_PLATFORM_BASE = _find_attr(_BASE, "_PLATFORM_BASE")


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _local_profile_id(request: Request) -> int:
    raw_id = request.headers.get("X-Rec-Room-Profile") or request.headers.get("x-rec-room-profile")
    try:
        player_id = int(raw_id or 0)
    except Exception:
        player_id = 0
    if player_id <= 0:
        raise HTTPException(status_code=400, detail="X-Rec-Room-Profile is required.")
    return player_id


def _player_payload(player: dict[str, Any]) -> dict[str, Any]:
    payload = dict(_BASE._serialize_profile_for_recnet(player))
    payload.setdefault("Developer", False)
    payload.setdefault("HasEmail", True)
    payload.setdefault("CanReceiveInvites", True)
    payload.setdefault("PhoneLastFour", "")
    return payload


def _platform_link_payload(player: dict[str, Any]) -> dict[str, Any]:
    state = player.get("state") or {}
    return {
        "Platform": int(state.get("platform") or 0),
        "PlatformId": str(state.get("platform_id") or "0"),
        "Player": _player_payload(player),
    }


def _ok_payload(message: str = "") -> dict[str, Any]:
    return {
        "Success": True,
        "Message": message,
        "HPOELBLNMPD": message,
    }


async def _json_or_form_payload(request: Request) -> Any:
    body = await request.body()
    if not body:
        return {}
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        return await _BASE._parse_client_payload(request)


async def _handle_current_player(request: Request, context) -> Response:
    player_id = _local_profile_id(request)
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    return JSONResponse(_player_payload(player))


async def _handle_list_by_platform_id(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    platform = _BASE._int_field(payload, "Platform", "platform", default=0)
    raw_ids = payload.get("PlatformIds") or payload.get("platformIds") or []
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="PlatformIds must be a list.")
    response: list[dict[str, Any]] = []
    for raw_platform_id in raw_ids:
        platform_id = str(raw_platform_id or "").strip()
        if not platform_id:
            continue
        player = _PLATFORM_BASE._find_player_by_platform(context, platform=platform, platform_id=platform_id)
        if player is None:
            continue
        context.assert_player_not_banned(player["player_id"])
        response.append(_platform_link_payload(player))
    return JSONResponse(response)


async def _handle_search_players(route_path: str, context) -> Response:
    clean_path = _clean_route_path(route_path)
    term = clean_path[len("api/players/v1/search/") :].strip()
    term = term.replace("+", " ")
    if not term:
        return JSONResponse([])
    needle = term.casefold()
    results: list[dict[str, Any]] = []
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players p
            JOIN player_version_state pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND p.is_banned = 0
              AND (
                    lower(p.username) LIKE ?
                 OR lower(p.display_name) LIKE ?
                 OR lower(json_extract(pvs.state_json, '$.name')) LIKE ?
              )
            ORDER BY p.updated_at DESC
            LIMIT 25
            """,
            (_PLATFORM_BASE.STATE_API_VERSION, f"%{needle}%", f"%{needle}%", f"%{needle}%"),
        ).fetchall()
    for row in rows:
        player = {key: row[key] for key in row.keys() if key != "state_json"}
        player["state"] = _PLATFORM_BASE._state_from_row(row)
        results.append(_player_payload(player))
    return JSONResponse(results)


async def _handle_display_name(request: Request, context) -> Response:
    player_id = _local_profile_id(request)
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    payload = await _BASE._parse_client_payload(request)
    new_name = _PLATFORM_BASE._safe_display_name(_BASE._str_field(payload, "Name", "name"), fallback=player["display_name"])
    state = player.get("state") or {}
    state["name"] = new_name
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET display_name = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
            """,
            (new_name, player["player_id"]),
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
    return JSONResponse(_ok_payload("Display name updated."))


async def _handle_phone(request: Request, context, *, verify: bool) -> Response:
    player_id = _local_profile_id(request)
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    payload = await _BASE._parse_client_payload(request)
    phone = _BASE._str_field(payload, "PhoneNumber", "Number", "phoneNumber", "number")
    state = player.get("state") or {}
    digits = re.sub(r"\D+", "", phone)
    if digits:
        state["phone_last_four"] = digits[-4:]
    if verify:
        state["phone_verified"] = True
    with context.db.transaction() as conn:
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
    return JSONResponse(_ok_payload("Phone verified." if verify else "Phone accepted."))


async def _handle_send_multiple(request: Request) -> Response:
    payload = await _json_or_form_payload(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="sendMultiple payload must be an object.")
    raw_ids = payload.get("ToPlayerIds") or payload.get("toPlayerIds") or []
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="ToPlayerIds must be a list.")
    return Response(status_code=204)


async def _handle_offline_invite(request: Request, context) -> Response:
    player_id = _local_profile_id(request)
    sender = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if sender is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(sender["player_id"])
    payload = await _BASE._parse_client_payload(request)
    target_id = _BASE._int_field(payload, "PlayerId", "playerId", "ToPlayerId", "toPlayerId", default=0)
    return JSONResponse({"Message": "", "HPOELBLNMPD": "", "PlayerId": target_id})


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path.startswith("api/playersubscriptions/"):
        raise HTTPException(status_code=404, detail="Unknown endpoint.")

    if path in {"api/players/v1", "api/players/v1/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Player method is not implemented.")
        return await _handle_current_player(request, context)

    if path in {"api/players/v1/listbyplatformid", "api/players/v1/listbyplatformid/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player platform-list method is not implemented.")
        return await _handle_list_by_platform_id(request, context)

    if path.startswith("api/players/v1/search/"):
        if method != "GET":
            raise HTTPException(status_code=501, detail="Player search method is not implemented.")
        return await _handle_search_players(route_path, context)

    if path in {"api/players/v2/displayname", "api/players/v2/displayname/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Display-name method is not implemented.")
        return await _handle_display_name(request, context)

    if path in {"api/players/v2/phone", "api/players/v2/phone/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Phone method is not implemented.")
        return await _handle_phone(request, context, verify=False)

    if path in {"api/players/v2/phone/verify", "api/players/v2/phone/verify/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Phone verify method is not implemented.")
        return await _handle_phone(request, context, verify=True)

    if path in {"api/messages/v1/sendmultiple", "api/messages/v1/sendmultiple/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="sendMultiple method is not implemented.")
        return await _handle_send_multiple(request)

    if path in {"api/offlineinvite/v1/send", "api/offlineinvite/v1/send/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Offline invite method is not implemented.")
        return await _handle_offline_invite(request, context)

    return await _SHARED.handle_http(request=request, route_path=route_path, context=context)


handle_websocket = _SHARED.handle_websocket
