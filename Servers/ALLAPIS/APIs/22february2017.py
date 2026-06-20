"""22 February 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 2859847576718116651:
- The v2/v3 account route family still matches the 14 February 2017 adapter.
- Objective completion moved from POST api/players/v2/objective to
  POST api/players/v1/objectives with a JSON array of objective completions.
- Player subscription synchronization is no longer the old REST
  api/PlayerSubscriptions/v1/init/add/remove surface; this client sends
  subscription list changes through the notification WebSocket.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response

API_VERSION = "22february2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Wed, 22 Feb 2017 19:52:07 GMT"


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
    module_path = Path(__file__).with_name("14february2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_14february2017_shared_for_22february2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 14february2017 adapter.")
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


def _objective_additional_xp(item: Any) -> int:
    if not isinstance(item, dict):
        return 0
    return max(0, _BASE._int_field(item, "additionalXp", "AdditionalXp", default=0))


async def _parse_objectives_payload(request: Request) -> list[dict[str, Any]]:
    body = await request.body()
    if not body:
        return []
    try:
        payload = json.loads(body.decode("utf-8", errors="replace"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid objectives payload.") from exc
    if isinstance(payload, dict):
        payload = payload.get("Objectives") or payload.get("objectives") or []
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="Objectives payload must be a list.")
    return [item for item in payload if isinstance(item, dict)]


async def _handle_objectives_v1(request: Request, context) -> Response:
    player_id = _local_profile_id(request)
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    objectives = await _parse_objectives_payload(request)
    additional_xp = sum(_objective_additional_xp(item) for item in objectives)
    completed_count = max(1, len(objectives))
    delta_xp = (_BASE.DEFAULT_XP_REWARD * completed_count) + additional_xp
    current_xp_total = max(0, int(player.get("canonical_xp") or 0)) + delta_xp
    current_level = max(1, current_xp_total // _BASE.XP_PER_LEVEL + 1)
    current_xp = current_xp_total % _BASE.XP_PER_LEVEL
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET canonical_xp = ?,
                canonical_level = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND is_coach = 0
            """,
            (current_xp, current_level, player["player_id"]),
        )
    return JSONResponse(
        {
            "deltaXp": delta_xp,
            "currentLevel": current_level,
            "currentXp": current_xp,
            "xpRequiredToLevelUp": _BASE.XP_PER_LEVEL,
        }
    )


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path.startswith("api/playersubscriptions/"):
        raise HTTPException(status_code=404, detail="Unknown endpoint.")

    if path in {"api/players/v2/objective", "api/players/v2/objective/"}:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")

    if path in {"api/players/v1/objectives", "api/players/v1/objectives/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player objectives method is not implemented.")
        return await _handle_objectives_v1(request, context)

    return await _SHARED.handle_http(request=request, route_path=route_path, context=context)


handle_websocket = _SHARED.handle_websocket
