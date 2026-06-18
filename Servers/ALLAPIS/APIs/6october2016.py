"""6 October 2016 Rec Room HTTP API adapter.

Confirmed from decompiled client build 1591673639619502502:
- GET  api/players/v1/?p=<platform>&id=<platform id>
- GET  api/players/v1/<Id>
- POST api/players/v1/create with form fields Platform, PlatformId, and Name
- POST api/players/v1/update/<Id> with WebManager.PlayerModel JSON
- POST api/players/v1/verify/<Id> with form field email
- GET  api/images/v1/profile/<Id>
- POST api/images/v1/profile/<Id> with multipart form field image
- GET  motd, for patched MOTD URL convenience
- GET  api/tournament?player=<Photon player name>
- GET  api/tournament/forfeit?match=<MatchId>&player=<Photon player name>

No non-Photon WebSocket API was found. The HTTP surface matches the
28 September adapter, so this module reuses that implementation while exposing
the real requested public route `/6october2016/...`.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from fastapi import Request, WebSocket
from fastapi.responses import Response

API_VERSION = "6october2016"


def _load_base_adapter():
    module_path = Path(__file__).with_name("28september2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_28september2016_shared_for_6october2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 28september2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module._BASE.API_VERSION = API_VERSION
    module._BASE._BASE.API_VERSION = API_VERSION
    module._BASE._PLATFORM_BASE.API_VERSION = API_VERSION
    return module


_BASE = _load_base_adapter()


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
