"""11 August 2016 v1 Rec Room HTTP API adapter.

Confirmed from decompiled client build 2459083052223685832:
- GET  api/players/?steamId=<ulong>
- POST api/players/ with WWWForm fields SteamID and Name
- PUT  api/players/<Id> with PlayerModel JSON
- GET  motd, for patched MOTD URL convenience

This build uses the same SteamID-era player schema as the shared 2016 legacy
adapter. Public routing stays /11august2016v1; player state is shared with
compatible 2016 builds.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from fastapi import Request, WebSocket
from fastapi.responses import Response

API_VERSION = "11august2016v1"


def _load_base_adapter():
    module_path = Path(__file__).with_name("17august2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_2016_steam_shared_for_11august2016v1", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load shared 2016 SteamID adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    return module


_BASE = _load_base_adapter()


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
