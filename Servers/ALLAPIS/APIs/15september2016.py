"""15 September 2016 Rec Room HTTP API adapter.

Confirmed from decompiled client build 6639491001730954404:
- GET  api/players/v1/?p=<platform>&id=<platform id>
- GET  api/players/v1/<Id>
- POST api/players/v1/create with form fields Platform, PlatformId, and Name
- POST api/players/v1/update/<Id> with WebManager.PlayerModel JSON
- GET  api/images/v1/profile/<Id>
- POST api/images/v1/profile/<Id> with multipart form field image
- GET  motd, for patched MOTD URL convenience
- GET  api/tournament?player=<Photon player name>
- GET  api/tournament/forfeit?match=<MatchId>&player=<Photon player name>

No non-Photon WebSocket API was found. Most of the player, image, MOTD, and
tournament surface matches the 8 September adapter. Confirmed 15 September-only
request handling stays in this module instead of changing older adapters.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import Response
from starlette import status

API_VERSION = "15september2016"


def _load_base_adapter():
    module_path = Path(__file__).with_name("8september2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_8september2016_shared_for_15september2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 8september2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    return module


_BASE = _load_base_adapter()
_PLATFORM_BASE = _BASE._BASE


def _route(route_path: str) -> str:
    return route_path.strip("/").casefold()


def _parse_multipart_fields(body: bytes, content_type: str) -> dict[str, str]:
    match = re.search(r"boundary=([^;]+)", content_type, flags=re.IGNORECASE)
    if not match:
        return {}
    boundary = match.group(1).strip().strip('"').encode("utf-8")
    delimiter = b"--" + boundary
    fields: dict[str, str] = {}
    for part in body.split(delimiter):
        part = part.strip(b"\r\n")
        if not part or part == b"--" or b"\r\n\r\n" not in part:
            continue
        raw_headers, raw_value = part.split(b"\r\n\r\n", 1)
        header_text = raw_headers.decode("utf-8", errors="replace")
        name_match = re.search(r'name="([^"]+)"', header_text)
        if not name_match:
            continue
        value = raw_value.rstrip(b"\r\n-").decode("utf-8", errors="replace")
        fields[name_match.group(1)] = value
    return fields


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


async def _handle_create_player(request: Request, context) -> Response:
    content_type = str(request.headers.get("content-type") or "")
    if "multipart/form-data" not in content_type.casefold():
        return await _BASE.handle_http(request=request, route_path="api/players/v1/create", context=context)

    payload = _parse_multipart_fields(await request.body(), content_type)
    platform = _int_field(payload, "Platform", "platform", "p", default=0)
    platform_id = _str_field(payload, "PlatformId", "platformId", "platform_id", "id")
    if not platform_id:
        raise HTTPException(status_code=400, detail="PlatformId is required.")
    context.assert_identities_not_banned(_PLATFORM_BASE._identity_pairs(platform, platform_id))
    existing = _PLATFORM_BASE._find_player_by_platform(context, platform=platform, platform_id=platform_id)
    if existing is not None:
        return _PLATFORM_BASE._json_response(existing)
    name = _str_field(payload, "Name", "name", default=f"Player{platform_id[-4:]}")
    player = _PLATFORM_BASE._create_player_for_platform(context, platform=platform, platform_id=platform_id, name=name)
    context.record_player_identities(player["player_id"], _PLATFORM_BASE._identity_pairs(platform, platform_id))
    return _PLATFORM_BASE._json_response(player, status_code=status.HTTP_201_CREATED)


async def _handle_get_player_by_legacy_id(route_path: str, context) -> Response:
    match = re.fullmatch(r"api/players/v1/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, int(match.group(1)))
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    return _PLATFORM_BASE._json_response(player)


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _route(route_path)
    if path in {"api/players/v1/create", "api/players/v1/create/"}:
        if request.method != "POST":
            raise HTTPException(status_code=501, detail="Player create method is not implemented.")
        return await _handle_create_player(request, context)
    if request.method == "GET" and re.fullmatch(r"api/players/v1/\d+/?", path):
        return await _handle_get_player_by_legacy_id(route_path, context)
    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
