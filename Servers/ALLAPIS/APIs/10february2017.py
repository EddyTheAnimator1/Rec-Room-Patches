"""10 February 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 2420201992531381519:
- RecNet networking is obfuscated, but the real HTTP base fields still route
  through the same request queue as the 3 February 2017 client family.
- Player subscription sync was added at api/PlayerSubscriptions/v1/add and
  api/PlayerSubscriptions/v1/remove with a raw JSON array of player ids.
- Objective leaderboard lookup was added at api/Leaderboard/v1.
- Push notifications use api/notification/v2.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse, Response

API_VERSION = "10february2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Fri, 10 Feb 2017 01:48:05 GMT"


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
    module_path = Path(__file__).with_name("3february2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_3february2017_shared_for_10february2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 3february2017 adapter.")
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
_BASE = _find_attr(_SHARED, "_BASE")


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _local_profile_id(request: Request) -> int:
    raw_id = request.headers.get("X-Rec-Room-Profile") or request.headers.get("x-rec-room-profile")
    try:
        return max(0, int(raw_id or 0))
    except Exception:
        return 0


def _subscription_key(player_id: int) -> str:
    owner = player_id if player_id > 0 else "anonymous"
    return _BASE._setting_key("player_subscriptions", owner)


def _leaderboard_row(player_id: int, count: int, order: int) -> dict[str, int]:
    return {"PlayerId": player_id, "Count": count, "Order": order}


def _empty_leaderboard_payload() -> dict[str, Any]:
    next_reset = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return {
        "GlobalOverall": [],
        "GlobalPeriodic": [],
        "FriendsOverall": [],
        "FriendsPeriodic": [],
        "NextResetUTC": next_reset.isoformat().replace("+00:00", "Z"),
    }


async def _handle_leaderboard(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    limit = _BASE._int_field(payload, "Limit", "limit", default=0)
    limit = max(0, min(limit, 100))
    local_id = _local_profile_id(request)
    response = _empty_leaderboard_payload()
    if local_id > 0 and limit > 0:
        row = _leaderboard_row(local_id, 0, 1)
        response["FriendsOverall"] = [row]
        response["FriendsPeriodic"] = [row]
    return JSONResponse(response)


async def _parse_subscription_ids(request: Request) -> list[int]:
    body = await request.body()
    if not body:
        return []
    try:
        payload = json.loads(body.decode("utf-8", errors="replace"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid subscription payload.") from exc
    if isinstance(payload, dict):
        payload = payload.get("PlayerIds") or payload.get("playerIds") or []
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="Subscription payload must be a list.")
    ids: list[int] = []
    for value in payload:
        try:
            player_id = int(value)
        except Exception:
            continue
        if player_id > 0:
            ids.append(player_id)
    return ids


async def _handle_player_subscriptions(request: Request, action: str, context) -> Response:
    player_id = _local_profile_id(request)
    requested_ids = await _parse_subscription_ids(request)
    key = _subscription_key(player_id)
    subscribed: set[int] = set()
    for value in _BASE._get_json_setting(context, key, []):
        try:
            stored_id = int(value)
        except Exception:
            continue
        if stored_id > 0:
            subscribed.add(stored_id)
    if action == "add":
        subscribed.update(requested_ids)
    elif action == "remove":
        subscribed.difference_update(requested_ids)
    else:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    _BASE._set_json_setting(context, key, sorted(subscribed))
    return Response(status_code=204)


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path in {"api/leaderboard/v1", "api/leaderboard/v1/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Leaderboard method is not implemented.")
        return await _handle_leaderboard(request, context)

    for action in ("add", "remove"):
        if path in {f"api/playersubscriptions/v1/{action}", f"api/playersubscriptions/v1/{action}/"}:
            if method != "POST":
                raise HTTPException(status_code=501, detail="Player subscription method is not implemented.")
            return await _handle_player_subscriptions(request, action, context)

    return await _SHARED.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _SHARED.handle_websocket(websocket=websocket, route_path=route_path, context=context)
