"""31 August 2016 v2 Rec Room HTTP API adapter.

Confirmed from decompiled client build 3798172550462140867:
- GET  api/players/v1/?p=<platform>&id=<platform id>
- POST api/players/v1/create with form fields Platform, PlatformId, and Name
- POST api/players/v1/update/<Id> with WebManager.PlayerModel JSON
- GET  motd, for patched MOTD URL convenience
- GET  api/tournament?player=<Photon player name>
- GET  api/tournament/forfeit?match=<MatchId>&player=<Photon player name>

The API-bearing classes are byte-for-byte identical to 31august2016. This
module reuses that implementation with version-specific state keys.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from fastapi import Request, WebSocket
from fastapi.responses import Response

API_VERSION = "31august2016v2"
NEXT_PLAYER_ID_SETTING = f"{API_VERSION}.next_legacy_player_id"


def _load_base_adapter():
    module_path = Path(__file__).with_name("31august2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_31august2016_shared_for_v2", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 31august2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module.NEXT_PLAYER_ID_SETTING = NEXT_PLAYER_ID_SETTING
    return module


_BASE = _load_base_adapter()


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
