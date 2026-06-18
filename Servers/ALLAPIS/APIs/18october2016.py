"""18 October 2016 Rec Room HTTP API adapter.

Confirmed from decompiled client build 5317305413924462757:
- GET  api/players/v1/?p=<platform>&id=<platform id>
- GET  api/players/v1/<Id>
- POST api/players/v1/create with form fields Platform, PlatformId, and Name
- POST api/players/v1/update/<Id> with WebManager.PlayerModel JSON
- POST api/players/v1/verify/<Id> with form field email
- GET  api/images/v1/profile/<Id>
- POST api/images/v1/profile/<Id> with multipart form field image
- GET  api/config/v1/motd
- GET  api/config/v1/objectives
- GET  api/tournament?player=<Photon player name>
- GET  api/tournament/forfeit?match=<MatchId>&player=<Photon player name>

No non-Photon WebSocket API was found. The player, image, verify, and
tournament surface matches the 7 October adapter. This build moves MOTD and
daily objectives into the config API, so those endpoints are handled here.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse, PlainTextResponse, Response

API_VERSION = "18october2016"

DAILY_OBJECTIVES = [
    [
        {"type": 301, "score": 1, "xp": 100},
        {"type": 302, "score": 3, "xp": 100},
        {"type": 400, "score": 1, "xp": 100},
    ],
    [
        {"type": 201, "score": 1, "xp": 100},
        {"type": 202, "score": 1, "xp": 100},
        {"type": 802, "score": 1, "xp": 100},
    ],
    [
        {"type": 500, "score": 1, "xp": 100},
        {"type": 502, "score": 3, "xp": 100},
        {"type": 801, "score": 1, "xp": 100},
    ],
    [
        {"type": 100, "score": 1, "xp": 100},
        {"type": 402, "score": 3, "xp": 100},
        {"type": 300, "score": 1, "xp": 100},
    ],
    [
        {"type": 600, "score": 1, "xp": 100},
        {"type": 603, "score": 1, "xp": 100},
        {"type": 200, "score": 1, "xp": 100},
    ],
    [
        {"type": 700, "score": 1, "xp": 100},
        {"type": 702, "score": 3, "xp": 100},
        {"type": 401, "score": 1, "xp": 100},
    ],
    [
        {"type": 501, "score": 1, "xp": 100},
        {"type": 601, "score": 1, "xp": 100},
        {"type": 802, "score": 1, "xp": 100},
    ],
]


def _load_base_adapter():
    module_path = Path(__file__).with_name("7october2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_7october2016_shared_for_18october2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 7october2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module._BASE.API_VERSION = API_VERSION
    module._BASE._BASE.API_VERSION = API_VERSION
    module._BASE._BASE._BASE.API_VERSION = API_VERSION
    module._BASE._BASE._BASE._BASE.API_VERSION = API_VERSION
    module._BASE._BASE._BASE._PLATFORM_BASE.API_VERSION = API_VERSION
    return module


_BASE = _load_base_adapter()


def _route(route_path: str) -> str:
    return route_path.strip("/").casefold()


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _route(route_path)
    if path in {"api/config/v1/motd", "api/config/v1/motd/"}:
        if request.method != "GET":
            raise HTTPException(status_code=501, detail="MOTD method is not implemented.")
        return PlainTextResponse(context.get_motd(API_VERSION), media_type="text/plain; charset=utf-8")
    if path in {"api/config/v1/objectives", "api/config/v1/objectives/"}:
        if request.method != "GET":
            raise HTTPException(status_code=501, detail="Daily objectives method is not implemented.")
        return JSONResponse(DAILY_OBJECTIVES)
    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
