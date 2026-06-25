"""23 March 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from the game build at manifest 4635637071237364407:
- RecNet still uses COGCNMJCNKN.
- HTTP/WebSocket URL fields remain EHBCBOGDLDB and FPGKGDJLOJJ.
- Login posts /api/platformlogin/v1 with the same form fields as 17 March.
- Profile image upload is POST api/images/v2/profile.
- Profile image display reads ProfileImageName from profile JSON, then downloads
  that path against config CdnBaseUri with COGCNMJCNKN.ELHLFBNMPMF.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import BASE as _SERVER_BASE
from fastapi import Request
from fastapi.responses import JSONResponse, Response

API_VERSION = "23march2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Thu, 23 Mar 2017 03:01:13 GMT"
MATCHMAKING_DEFAULTS = {
    "PreferFullRoomsFrequency": 0.5,
    "PreferEmptyRoomsFrequency": 0.0,
}
LEVEL_PROGRESSION_MAPS = [{"Level": level, "RequiredXp": (level - 1) * 500} for level in range(1, 31)]
DAILY_OBJECTIVES_DEFAULTS = [
    [{"type": 301, "score": 1}, {"type": 500, "score": 1}, {"type": 801, "score": 1}],
    [{"type": 201, "score": 1}, {"type": 400, "score": 1}, {"type": 100, "score": 1}],
    [{"type": 601, "score": 1}, {"type": 701, "score": 1}, {"type": 301, "score": 1}],
    [{"type": 801, "score": 1}, {"type": 201, "score": 1}, {"type": 500, "score": 1}],
    [{"type": 100, "score": 1}, {"type": 400, "score": 1}, {"type": 301, "score": 1}],
    [{"type": 500, "score": 1}, {"type": 801, "score": 1}, {"type": 201, "score": 1}],
    [{"type": 301, "score": 1}, {"type": 400, "score": 1}, {"type": 100, "score": 1}],
]
PHOTON_CONFIG_DEFAULTS = {
    "CloudRegion": "us",
    "CrcCheckEnabled": False,
}


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
    module_path = Path(__file__).with_name("17march2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_17march2017_shared_for_23march2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 17march2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()
_BASE = _SHARED._BASE


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _add_profile_image_names(payload: Any) -> bool:
    if isinstance(payload, list):
        changed = False
        for item in payload:
            changed = _add_profile_image_names(item) or changed
        return changed

    if not isinstance(payload, dict):
        return False

    changed = False
    player_payload = payload.get("Player")
    if isinstance(player_payload, dict):
        changed = _add_profile_image_names(player_payload) or changed

    raw_id = payload.get("Id") or payload.get("PlayerId")
    if raw_id and ("Username" in payload or "DisplayName" in payload):
        try:
            player_id = int(raw_id)
        except Exception:
            player_id = 0
        if player_id > 0 and not payload.get("ProfileImageName"):
            payload["ProfileImageName"] = f"api/images/v1/profile/{player_id}"
            changed = True
    return changed


def _add_config_fields(payload: Any, request: Request, context) -> bool:
    if not isinstance(payload, dict):
        return False
    changed = _add_profile_image_names(payload)
    server_base = _SERVER_BASE.public_api_base_url(request, context, API_VERSION)

    if not isinstance(payload.get("MessageOfTheDay"), str):
        payload["MessageOfTheDay"] = ""
        changed = True

    for key in ("CdnBaseUri", "serverAddress"):
        if payload.get(key) != server_base:
            payload[key] = server_base
            changed = True

    matchmaking_params = payload.get("MatchmakingParams")
    if not isinstance(matchmaking_params, dict):
        payload["MatchmakingParams"] = dict(MATCHMAKING_DEFAULTS)
        changed = True
    else:
        for key, value in MATCHMAKING_DEFAULTS.items():
            if key not in matchmaking_params:
                matchmaking_params[key] = value
                changed = True

    level_maps = payload.get("LevelProgressionMaps")
    if not isinstance(level_maps, list) or not level_maps or not all(
        isinstance(item, dict) and "Level" in item and "RequiredXp" in item for item in level_maps
    ):
        payload["LevelProgressionMaps"] = [dict(item) for item in LEVEL_PROGRESSION_MAPS]
        changed = True

    daily_objectives = payload.get("DailyObjectives")
    if (
        not isinstance(daily_objectives, list)
        or not daily_objectives
        or not all(
            isinstance(day, list)
            and all(isinstance(item, dict) and "type" in item and "score" in item for item in day)
            for day in daily_objectives
        )
    ):
        payload["DailyObjectives"] = [[dict(item) for item in day] for day in DAILY_OBJECTIVES_DEFAULTS]
        changed = True

    config_table = payload.get("ConfigTable")
    if not isinstance(config_table, list) or not all(isinstance(item, dict) for item in config_table):
        payload["ConfigTable"] = []
        changed = True

    photon_config = payload.get("PhotonConfig")
    if not isinstance(photon_config, dict):
        payload["PhotonConfig"] = dict(PHOTON_CONFIG_DEFAULTS)
        changed = True
    else:
        for key, value in PHOTON_CONFIG_DEFAULTS.items():
            if key not in photon_config:
                photon_config[key] = value
                changed = True
    return changed


async def _handle_config_v2(request: Request, route_path: str, context) -> Response:
    response = await _SHARED.handle_http(request=request, route_path=route_path, context=context)
    payload = _BASE._load_response_json(response)
    if not _add_config_fields(payload, request, context):
        return response
    return JSONResponse(payload, status_code=getattr(response, "status_code", 200))


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path in {"api/config/v2", "api/config/v2/"}:
        if method != "GET":
            return await _SHARED.handle_http(request=request, route_path=route_path, context=context)
        return await _handle_config_v2(request, route_path, context)

    response = await _SHARED.handle_http(request=request, route_path=route_path, context=context)
    if path.startswith("api/players/"):
        payload = _BASE._load_response_json(response)
        if _add_profile_image_names(payload):
            return JSONResponse(payload, status_code=getattr(response, "status_code", 200))
    return response


handle_websocket = _SHARED.handle_websocket
