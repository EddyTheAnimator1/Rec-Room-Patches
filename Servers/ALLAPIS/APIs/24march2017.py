"""24 March 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from the game build at manifest 1655320907991352027:
- RecNet still uses COGCNMJCNKN.
- HTTP/WebSocket URL fields remain EHBCBOGDLDB and FPGKGDJLOJJ.
- The RecNet route surface matches the 23 March 2017 v2 client family.
- Server config is still GET api/config/v2 and includes serverAddress.
"""

from __future__ import annotations

import importlib.util
import json
import time
from pathlib import Path

from fastapi import HTTPException, WebSocketDisconnect
from fastapi.responses import JSONResponse

API_VERSION = "24march2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Fri, 24 Mar 2017 00:50:23 GMT"
DEFAULT_SAFE_AVATAR = {
    "OutfitSelections": (
        "06306723-ca20-4aa6-b7b3-917113f41ac3,,,,0;"
        "8d10cc78-6b00-45f3-affb-205e9cc5b03f,,,,0;"
        "21caa68e-c3fa-474c-af5e-af1e742b7a60,,,,1;"
        "ecc1dbe6-ca06-4564-b2a6-30956194d1e9,51ef8d39-2b94-4f9e-9620-07b6b0a913a5,0b2395e1-ebcc-47e9-aaf1-faf9e9cec4cd,,2;"
        "ecc1dbe6-ca06-4564-b2a6-30956194d1e9,51ef8d39-2b94-4f9e-9620-07b6b0a913a5,0b2395e1-ebcc-47e9-aaf1-faf9e9cec4cd,,3;"
        "40528de7-38a3-4a7c-8f93-6d3bfa5573f2,dee70c38-7a99-4c2b-9181-665f1bf75aca,018a5c07-e956-457d-a540-a5e2cd68da09,,0"
    ),
    "HairColor": "5ee30295-b05f-4e96-819e-5ac865b2c63d",
    "SkinColor": "2d398478-37c4-4c4a-a471-fbcbe3e5b1f5",
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
    module_path = Path(__file__).with_name("23march2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_23march2017_shared_for_24march2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 23march2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()

handle_http = _SHARED.handle_http


def _find_shared_module(name: str):
    module = _SHARED
    while module is not None:
        if hasattr(module, name):
            return module
        module = getattr(module, "_SHARED", None)
    raise RuntimeError(f"Shared adapter does not expose {name}.")


_NOTIFICATION_BASE = _find_shared_module("_json_object_from_text")
_clean_route_path = _SHARED._clean_route_path


def _is_empty_avatar(payload) -> bool:
    if not isinstance(payload, dict):
        return False
    return not any(str(payload.get(key) or "").strip() for key in ("OutfitSelections", "HairColor", "SkinColor"))


async def handle_http(*, request, route_path: str, context):
    response = await _SHARED.handle_http(request=request, route_path=route_path, context=context)
    path = _clean_route_path(route_path).casefold()
    if request.method.upper() == "GET" and path in {"api/avatar/v2", "api/avatar/v2/"}:
        payload = _NOTIFICATION_BASE._BASE._load_response_json(response)
        if _is_empty_avatar(payload):
            return JSONResponse(dict(DEFAULT_SAFE_AVATAR), status_code=getattr(response, "status_code", 200))
    return response


async def _close_websocket(websocket, code: int, reason: str = "") -> None:
    try:
        await websocket.close(code=code, reason=reason[:120])
    except Exception:
        pass


async def handle_websocket(*, websocket, route_path: str, context) -> None:
    path = _clean_route_path(route_path).casefold()
    if path not in {"api/notification/v2", "api/notification/v2/"}:
        await _SHARED.handle_websocket(websocket=websocket, route_path=route_path, context=context)
        return

    await websocket.accept()
    player_id = 0
    try:
        handshake = _NOTIFICATION_BASE._json_object_from_text(await websocket.receive_text())
        player_id = _SHARED._local_profile_id(websocket)
        if player_id <= 0:
            raise HTTPException(status_code=400, detail="X-Rec-Room-Profile is required.")

        _NOTIFICATION_BASE._BASE._ensure_existing_profile(context, player_id)
        await _NOTIFICATION_BASE._register_notification_client(websocket, player_id)
        session_id = int(time.time() * 1000)
        await websocket.send_text(json.dumps({"SessionId": session_id}))
        await _NOTIFICATION_BASE._publish_presence(context, player_id, handshake)
        while True:
            await _NOTIFICATION_BASE._handle_notification_client_message(
                await websocket.receive_text(),
                player_id,
                context,
            )
    except WebSocketDisconnect:
        await _NOTIFICATION_BASE._unregister_notification_client(websocket, context)
    except HTTPException as exc:
        await _NOTIFICATION_BASE._unregister_notification_client(websocket, context)
        await _close_websocket(websocket, 1008, str(exc.detail))
    except Exception:
        await _NOTIFICATION_BASE._unregister_notification_client(websocket, context)
        raise
