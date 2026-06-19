"""4 November 2016 Rec Room HTTP API adapter.

Confirmed from decompiled client build 976486110269581733:
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

No non-Photon WebSocket API was found. The verified RecNet profile/config,
image, and tournament HTTP surface matches 29 October 2016, including the
strict RecNet.Profile response schema and server-config daily objectives.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from fastapi import Request, WebSocket
from fastapi.responses import Response

API_VERSION = "4november2016"


def _set_api_version(module) -> None:
    seen: set[int] = set()
    stack = [module]
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if hasattr(current, "API_VERSION"):
            current.API_VERSION = API_VERSION
        for attr in ("_BASE", "_PLATFORM_BASE"):
            child = getattr(current, attr, None)
            if child is not None:
                stack.append(child)


def _load_base_adapter():
    module_path = Path(__file__).with_name("29october2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_29october2016_shared_for_4november2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 29october2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _set_api_version(module)
    return module


_BASE = _load_base_adapter()


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
