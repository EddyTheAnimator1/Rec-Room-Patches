"""29 October 2016 Rec Room HTTP API adapter.

Confirmed from decompiled client build 5383238034238139872:
- GET  api/players/v1/?p=<platform>&id=<platform id>
- GET  api/players/v1/<Id>
- POST api/players/v1/create with form fields Platform, PlatformId, and Name
- POST api/players/v1/update/<Id> with RecNet.Profile JSON
- POST api/players/v1/verify/<Id> with form field email
- GET  api/images/v1/profile/<Id>
- POST api/images/v1/profile/<Id> with multipart form field image
- GET  api/config/v1/motd
- GET  api/config/v1/objectives
- GET  api/tournament?player=<Photon player name>
- GET  api/tournament/forfeit?match=<MatchId>&player=<Photon player name>

No non-Photon WebSocket API was found. This build uses the newer
RecNet.Core/Profile/Config/Images wrappers and the strict RecNet.Profile JSON
schema introduced before this date.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse, PlainTextResponse, Response

API_VERSION = "29october2016"

DAILY_OBJECTIVES = [
    [
        {"type": 301, "score": 1, "xp": 100},  # DodgeballGames
        {"type": 500, "score": 1, "xp": 100},  # PaintballAnyModeGames
        {"type": 801, "score": 1, "xp": 100},  # SoccerGames
    ],
    [
        {"type": 201, "score": 1, "xp": 100},  # DiscGolfGames
        {"type": 400, "score": 1, "xp": 100},  # PaddleballGames
        {"type": 100, "score": 1, "xp": 100},  # CharadesGames
    ],
    [
        {"type": 601, "score": 1, "xp": 100},  # PaintballCTFGames
        {"type": 701, "score": 1, "xp": 100},  # PaintballTeamBattleGames
        {"type": 301, "score": 1, "xp": 100},  # DodgeballGames
    ],
    [
        {"type": 801, "score": 1, "xp": 100},  # SoccerGames
        {"type": 201, "score": 1, "xp": 100},  # DiscGolfGames
        {"type": 500, "score": 1, "xp": 100},  # PaintballAnyModeGames
    ],
    [
        {"type": 100, "score": 1, "xp": 100},  # CharadesGames
        {"type": 400, "score": 1, "xp": 100},  # PaddleballGames
        {"type": 301, "score": 1, "xp": 100},  # DodgeballGames
    ],
    [
        {"type": 500, "score": 1, "xp": 100},  # PaintballAnyModeGames
        {"type": 801, "score": 1, "xp": 100},  # SoccerGames
        {"type": 201, "score": 1, "xp": 100},  # DiscGolfGames
    ],
    [
        {"type": 301, "score": 1, "xp": 100},  # DodgeballGames
        {"type": 400, "score": 1, "xp": 100},  # PaddleballGames
        {"type": 100, "score": 1, "xp": 100},  # CharadesGames
    ],
]


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _load_response_json(response: Response) -> Any:
    body = getattr(response, "body", b"")
    if isinstance(body, str):
        body = body.encode("utf-8")
    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        return None


def _load_base_adapter():
    module_path = Path(__file__).with_name("18october2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_18october2016_shared_for_29october2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 18october2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module._BASE.API_VERSION = API_VERSION
    module._BASE._BASE.API_VERSION = API_VERSION
    module._BASE._BASE._BASE.API_VERSION = API_VERSION
    module._BASE._BASE._BASE._BASE.API_VERSION = API_VERSION
    module._BASE._BASE._BASE._BASE._BASE.API_VERSION = API_VERSION
    module._BASE._BASE._BASE._BASE._PLATFORM_BASE.API_VERSION = API_VERSION
    return module


_BASE = _load_base_adapter()


def _find_platform_base(module):
    seen: set[int] = set()
    stack = [module]
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if hasattr(current, "_find_player_by_platform") and hasattr(current, "_serialize_player_for_client"):
            return current
        for attr in ("_PLATFORM_BASE", "_BASE"):
            child = getattr(current, attr, None)
            if child is not None:
                stack.append(child)
    raise RuntimeError("Could not find shared platform player adapter.")


_PLATFORM_BASE = _find_platform_base(_BASE)


def _int_field(payload: dict[str, Any], *names: str, default: int = 0) -> int:
    for name in names:
        if name in payload and payload[name] is not None:
            try:
                return int(payload[name])
            except Exception:
                return default
    return default


def _str_field(payload: dict[str, Any], *names: str, default: str = "") -> str:
    for name in names:
        if name in payload and payload[name] is not None:
            return str(payload[name]).strip()
    return default


def _bool_field(payload: dict[str, Any], *names: str, default: bool = False) -> bool:
    for name in names:
        if name in payload and payload[name] is not None:
            value = payload[name]
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(value)
            if isinstance(value, str):
                return value.strip().casefold() in {"1", "true", "yes", "on"}
            return bool(value)
    return default


def _serialize_profile_for_recnet(player: dict[str, Any]) -> dict[str, Any]:
    state = player.get("state") or {}
    username = str(player.get("display_name") or player.get("username") or state.get("name") or "Player")
    return {
        "Id": int(state.get("legacy_player_id") or 0),
        "Username": username,
        "XP": int(player.get("canonical_xp") or 0),
        "Level": max(1, int(player.get("canonical_level") or 1)),
        "Reputation": int(state.get("reputation") or _PLATFORM_BASE.DEFAULT_REPUTATION),
        "Verified": bool(player.get("verified")),
    }


def _json_profile_response(player: dict[str, Any], *, status_code: int = 200) -> Response:
    return JSONResponse(_serialize_profile_for_recnet(player), status_code=status_code)


def _legacy_profile_to_recnet(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or "Id" not in payload:
        return None
    return {
        "Id": int(payload.get("Id") or 0),
        "Username": str(payload.get("Username") or payload.get("Name") or "Player"),
        "XP": int(payload.get("XP") or 0),
        "Level": max(1, int(payload.get("Level") or 1)),
        "Reputation": int(payload.get("Reputation") or _PLATFORM_BASE.DEFAULT_REPUTATION),
        "Verified": bool(payload.get("Verified")),
    }


async def _handle_get_profile_by_platform(request: Request, context) -> Response:
    platform = _int_field(dict(request.query_params), "p", "Platform", "platform", default=0)
    platform_id = str(request.query_params.get("id") or request.query_params.get("PlatformId") or "").strip()
    if not platform_id:
        raise HTTPException(status_code=400, detail="id is required.")
    context.assert_identities_not_banned(_PLATFORM_BASE._identity_pairs(platform, platform_id))
    player = _PLATFORM_BASE._find_player_by_platform(context, platform=platform, platform_id=platform_id)
    if player is None:
        return Response(content="null", media_type="application/json")
    context.assert_player_not_banned(player["player_id"])
    context.record_player_identities(player["player_id"], _PLATFORM_BASE._identity_pairs(platform, platform_id))
    return _json_profile_response(player)


async def _handle_get_profile_by_id(route_path: str, context) -> Response:
    match = re.fullmatch(r"api/players/v1/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, int(match.group(1)))
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    return _json_profile_response(player)


async def _handle_create_profile(request: Request, route_path: str, context) -> Response:
    response = await _BASE.handle_http(request=request, route_path=route_path, context=context)
    payload = _load_response_json(response)
    profile = _legacy_profile_to_recnet(payload)
    if profile is None:
        return response
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, int(profile["Id"]))
    if player is None:
        return JSONResponse(profile, status_code=getattr(response, "status_code", 200))
    return _json_profile_response(player, status_code=getattr(response, "status_code", 200))


async def _handle_update_profile(request: Request, route_path: str, context) -> Response:
    match = re.fullmatch(r"api/players/v1/update/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    legacy_id = int(match.group(1))
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])

    payload = await _PLATFORM_BASE._parse_client_payload(request)
    state = player.get("state") or {}
    display_name = _str_field(
        payload,
        "Username",
        "Name",
        "name",
        default=str(state.get("name") or player.get("display_name") or "Player"),
    )
    state["name"] = _PLATFORM_BASE._safe_display_name(display_name, fallback=str(state.get("name") or "Player"))
    state["platform"] = _int_field(payload, "Platform", "platform", default=int(state.get("platform") or 0))
    state["platform_id"] = _str_field(
        payload,
        "PlatformId",
        "platformId",
        "platform_id",
        default=str(state.get("platform_id") or "0"),
    )
    state["reputation"] = _int_field(
        payload,
        "Reputation",
        "reputation",
        default=int(state.get("reputation") or _PLATFORM_BASE.DEFAULT_REPUTATION),
    )
    xp = _int_field(payload, "XP", "xp", default=int(player.get("canonical_xp") or 0))
    level = _int_field(payload, "Level", "level", default=int(player.get("canonical_level") or 1))
    verified = _bool_field(payload, "Verified", "verified", default=bool(player.get("verified")))

    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET display_name = ?,
                canonical_xp = ?,
                canonical_level = ?,
                verified = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND is_coach = 0
            """,
            (state["name"], max(0, xp), max(1, level), 1 if verified else 0, player["player_id"]),
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

    updated = _PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
    return _json_profile_response(updated or player)


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()

    if path in {"api/config/v1/motd", "api/config/v1/motd/"}:
        if request.method.upper() != "GET":
            raise HTTPException(status_code=501, detail="MOTD method is not implemented.")
        return PlainTextResponse(context.get_motd(API_VERSION), media_type="text/plain; charset=utf-8")
    if path in {"api/config/v1/objectives", "api/config/v1/objectives/"}:
        if request.method.upper() != "GET":
            raise HTTPException(status_code=501, detail="Daily objectives method is not implemented.")
        return JSONResponse(DAILY_OBJECTIVES)

    if request.method.upper() == "GET" and path in {"api/players/v1", "api/players/v1/"}:
        return await _handle_get_profile_by_platform(request, context)
    if request.method.upper() == "GET" and re.fullmatch(r"api/players/v1/\d+/?", path):
        return await _handle_get_profile_by_id(route_path, context)
    if path in {"api/players/v1/create", "api/players/v1/create/"}:
        if request.method.upper() != "POST":
            raise HTTPException(status_code=501, detail="Player create method is not implemented.")
        return await _handle_create_profile(request, route_path, context)
    if path.startswith("api/players/v1/update/"):
        if request.method.upper() != "POST":
            raise HTTPException(status_code=501, detail="Player update method is not implemented.")
        return await _handle_update_profile(request, route_path, context)

    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
