"""18 January 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 4350859901318817364:
- The base route surface matches the 7/11 January 2017 client family.
- Player reputation healing was added at api/playerReputation/v1/heal.
- Player reporting was added at api/PlayerReporting/v1/create.
- Push notifications still use api/notification/v2.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse, Response

API_VERSION = "18january2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Wed, 18 Jan 2017 01:10:07 GMT"


def _load_shared_adapter():
    module_path = Path(__file__).with_name("9december2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_9december2016_shared_for_18january2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 9december2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    module._BASE.API_VERSION = API_VERSION
    module._BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    module._BASE._set_api_version(module._BASE)
    return module


_SHARED = _load_shared_adapter()
_BASE = _SHARED._BASE
_PLATFORM_BASE = _SHARED._PLATFORM_BASE
PROFILE_REQUIRED_KEYS = {"Id", "Username", "DisplayName", "XP", "Level", "Reputation", "Verified"}


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _looks_like_profile_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and PROFILE_REQUIRED_KEYS.issubset(payload)


def _add_developer_flag(payload: Any) -> bool:
    if _looks_like_profile_payload(payload):
        payload["Developer"] = True
        return True
    if isinstance(payload, list):
        changed = False
        for item in payload:
            if _looks_like_profile_payload(item):
                item["Developer"] = True
                changed = True
        return changed
    return False


async def _handle_shared_http(request: Request, route_path: str, context) -> Response:
    response = await _SHARED.handle_http(request=request, route_path=route_path, context=context)
    path = _clean_route_path(route_path).casefold()
    if not path.startswith("api/players/"):
        return response
    payload = _BASE._load_response_json(response)
    if not _add_developer_flag(payload):
        return response
    return JSONResponse(payload, status_code=getattr(response, "status_code", 200))


def _local_profile_id(request: Request, context=None) -> int:
    raw_id = request.headers.get("X-Rec-Room-Profile") or request.headers.get("x-rec-room-profile")
    try:
        player_id = int(raw_id or 0)
    except Exception:
        player_id = 0
    if player_id > 0:
        return player_id
    return _SHARED._fallback_profile_id(context) if context is not None else 1


async def _handle_reputation_heal(request: Request, context) -> Response:
    player_id = _local_profile_id(request, context)
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    payload = await _BASE._parse_client_payload(request)
    minutes = max(0, _BASE._int_field(payload, "GoodKarmaMinutes", "goodKarmaMinutes", default=0))
    state = player.get("state") or {}
    current = int(state.get("reputation") or _PLATFORM_BASE.DEFAULT_REPUTATION)
    state["reputation"] = min(_PLATFORM_BASE.DEFAULT_REPUTATION, current + minutes)
    state["goodKarmaMinutes"] = int(state.get("goodKarmaMinutes") or 0) + minutes
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
    return Response(status_code=204)


async def _handle_player_report(request: Request, context) -> Response:
    reporter_id = _local_profile_id(request, context)
    reporter = _PLATFORM_BASE._find_player_by_legacy_id(context, reporter_id)
    if reporter is None:
        raise HTTPException(status_code=404, detail="Reporter not found.")
    payload = await _BASE._parse_client_payload(request)
    reported_id = _BASE._int_field(payload, "PlayerIdReported", "playerIdReported", default=0)
    if reported_id <= 0:
        raise HTTPException(status_code=400, detail="PlayerIdReported is required.")
    _BASE._ensure_existing_profile(context, reported_id)
    reports = _BASE._get_json_setting(context, _BASE._setting_key("player_reports", "global"), [])
    reports.append(
        {
            "ReporterPlayerId": reporter_id,
            "PlayerIdReported": reported_id,
            "ReportCategory": _BASE._int_field(payload, "ReportCategory", "reportCategory", default=0),
            "Activity": _BASE._str_field(payload, "Activity", "activity"),
        }
    )
    _BASE._set_json_setting(context, _BASE._setting_key("player_reports", "global"), reports)
    return Response(status_code=204)


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path in {"api/playerreputation/v1/heal", "api/playerreputation/v1/heal/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player reputation heal method is not implemented.")
        return await _handle_reputation_heal(request, context)

    if path in {"api/playerreporting/v1/create", "api/playerreporting/v1/create/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player reporting method is not implemented.")
        return await _handle_player_report(request, context)

    return await _handle_shared_http(request, route_path, context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _SHARED.handle_websocket(websocket=websocket, route_path=route_path, context=context)
