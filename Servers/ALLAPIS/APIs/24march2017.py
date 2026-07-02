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
import re
import time
from pathlib import Path

from fastapi import HTTPException, WebSocketDisconnect
from fastapi.responses import JSONResponse

API_VERSION = "24march2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Fri, 24 Mar 2017 00:50:23 GMT"
DEFAULT_SAFE_AVATAR = {
    "OutfitSelections": "",
    "HairColor": "5ee30295-b05f-4e96-819e-5ac865b2c63d",
    "SkinColor": "2d398478-37c4-4c4a-a471-fbcbe3e5b1f5",
}
AVATAR_BODY_PARTS = {"0", "1", "2", "3", "4"}
GUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)


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


def _find_shared_module(name: str):
    module = _SHARED
    while module is not None:
        if hasattr(module, name):
            return module
        module = getattr(module, "_SHARED", None)
    raise RuntimeError(f"Shared adapter does not expose {name}.")


_NOTIFICATION_BASE = _find_shared_module("_json_object_from_text")
_clean_route_path = _SHARED._clean_route_path


def _is_guid(value: str) -> bool:
    return bool(GUID_RE.fullmatch(value.strip()))


def _clean_guid(value: object, default: str = "") -> str:
    text = str(value or "").strip()
    return text if _is_guid(text) else default


def _normalize_outfit_selections(value: object) -> str:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_selection in str(value or "").split(";"):
        parts = [part.strip() for part in raw_selection.split(",")]
        if len(parts) != 5:
            continue
        outfit, swatch, mask, decal, body_part = parts
        if not _is_guid(outfit) or body_part not in AVATAR_BODY_PARTS:
            continue
        if swatch and not _is_guid(swatch):
            continue
        if mask and not _is_guid(mask):
            continue
        if decal and not _is_guid(decal):
            continue
        selection = ",".join([outfit, swatch, mask, decal, body_part])
        if selection not in seen:
            normalized.append(selection)
            seen.add(selection)
    return ";".join(normalized)


def _normalize_avatar_payload(payload) -> dict[str, str]:
    if not isinstance(payload, dict):
        payload = {}
    return {
        "OutfitSelections": _normalize_outfit_selections(payload.get("OutfitSelections")),
        "HairColor": _clean_guid(payload.get("HairColor"), DEFAULT_SAFE_AVATAR["HairColor"]),
        "SkinColor": _clean_guid(payload.get("SkinColor"), DEFAULT_SAFE_AVATAR["SkinColor"]),
    }


async def handle_http(*, request, route_path: str, context):
    response = await _SHARED.handle_http(request=request, route_path=route_path, context=context)
    path = _clean_route_path(route_path).casefold()
    if request.method.upper() == "GET" and path in {"api/avatar/v2", "api/avatar/v2/"}:
        payload = _NOTIFICATION_BASE._BASE._load_response_json(response)
        return JSONResponse(_normalize_avatar_payload(payload), status_code=getattr(response, "status_code", 200))
    return response


async def _close_websocket(websocket, code: int, reason: str = "") -> None:
    try:
        await websocket.close(code=code, reason=reason[:120])
    except Exception:
        pass


def _registered_peer_ids(player_id: int) -> list[int]:
    # Keep 24 March startup quiet for the connecting client; peers still need presence updates.
    return [registered_id for registered_id in _NOTIFICATION_BASE._registered_player_ids() if registered_id != player_id]


async def _publish_presence_to_peers(context, player_id: int, payload: dict, *, default_online: bool = True) -> None:
    presence = _NOTIFICATION_BASE._presence_payload_from_client(context, player_id, payload, default_online=default_online)
    _NOTIFICATION_BASE._save_presence(context, player_id, presence)
    await _NOTIFICATION_BASE._broadcast_notification(
        _registered_peer_ids(player_id),
        _NOTIFICATION_BASE._SUBSCRIPTION_UPDATE_PRESENCE,
        presence,
    )


async def _handle_notification_client_message(raw_message: str, player_id: int, context) -> None:
    payload = _NOTIFICATION_BASE._json_object_from_text(raw_message)
    api = str(payload.get("api") or payload.get("Api") or payload.get("API") or "").strip("/")
    if api.casefold() != "presence/v1":
        return
    await _publish_presence_to_peers(
        context,
        player_id,
        _NOTIFICATION_BASE._coerce_json_object(payload.get("param") or payload.get("Param")),
    )


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
        await _publish_presence_to_peers(context, player_id, handshake)
        while True:
            await _handle_notification_client_message(
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
