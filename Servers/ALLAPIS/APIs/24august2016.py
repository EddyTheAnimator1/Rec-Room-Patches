"""24 August 2016 Rec Room HTTP API adapter.

Confirmed from decompiled client build 5360232407282512201:
- GET  api/players/?steamId=<ulong>
- POST api/players/ with WWWForm fields SteamID and Name
- PUT  api/players/<Id> with PlayerModel JSON
- GET  motd, for patched MOTD URL convenience
- GET  api/tournament?player=<Photon player name>
- GET  api/tournament/forfeit?match=<MatchId>&player=<Photon player name>

The player model is unchanged from the 17 August adapter. Tournament support is
implemented as a safe no-active-match/no-op stub because no canonical tournament
state exists in the shared data model yet.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import Response

API_VERSION = "24august2016"
NEXT_PLAYER_ID_SETTING = f"{API_VERSION}.next_legacy_player_id"


def _load_base_adapter():
    module_path = Path(__file__).with_name("17august2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_17august2016_shared_for_24august2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 17august2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module.NEXT_PLAYER_ID_SETTING = NEXT_PLAYER_ID_SETTING
    return module


def _route(route_path: str) -> str:
    return route_path.strip("/").casefold()


_BASE = _load_base_adapter()


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _route(route_path)

    if path in {"api/tournament", "api/tournament/"}:
        if request.method != "GET":
            raise HTTPException(status_code=501, detail="Tournament method is not implemented.")
        return Response(status_code=204)

    if path in {"api/tournament/forfeit", "api/tournament/forfeit/"}:
        if request.method != "GET":
            raise HTTPException(status_code=501, detail="Tournament forfeit method is not implemented.")
        return Response(status_code=204)

    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
