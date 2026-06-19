"""24 November 2016 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 8464629802564135940:
- RecNet HTTP/WebSocket files are byte-identical to 23 November 2016.
- PlayerObjectiveTracker still reads daily objectives by DateTime.Today.DayOfWeek.
- RecNet.Images still requires LAST-MODIFIED on 200 profile image responses.
- Startup still requires api/notification/v1 WebSocket text OK.

This build gets a dedicated version module instead of a BASE.py alias so the
route prefix, MOTD scope, settings scope, and scratch notes stay build-specific.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from fastapi import Request, WebSocket
from fastapi.responses import Response

API_VERSION = "24november2016"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Thu, 24 Nov 2016 02:55:54 GMT"


def _load_base_adapter():
    module_path = Path(__file__).with_name("23november2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_23november2016_shared_for_24november2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 23november2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    module._set_api_version(module)
    return module


_BASE = _load_base_adapter()


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
