"""9 December 2016 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 8224493981844824938:
- Startup now probes GET api/versioncheck/v1, downloads GET api/config/v2,
  then creates/loads the local profile through POST api/players/v1/getorcreate.
- Local-player endpoints now infer the player from X-Rec-Room-Profile.
- Avatar, image upload, settings, relationships, messages, and presence moved
  to v2/v3 routes while keeping the same underlying data shapes.
- Push notification WebSocket moved to api/notification/v2 and expects a JSON
  handshake response with a positive SessionId.
"""

from __future__ import annotations

import importlib.util
import json
import time
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response

API_VERSION = "9december2016"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Fri, 09 Dec 2016 03:11:41 GMT"


def _load_base_adapter():
    module_path = Path(__file__).with_name("23november2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_23november2016_shared_for_9december2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 23november2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    module._set_api_version(module)
    return module


_BASE = _load_base_adapter()
_PLATFORM_BASE = _BASE._PLATFORM_BASE


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _local_profile_id(request: Request) -> int:
    raw_id = request.headers.get("X-Rec-Room-Profile") or request.headers.get("x-rec-room-profile")
    try:
        player_id = int(raw_id or 0)
    except Exception:
        player_id = 0
    if player_id <= 0:
        raise HTTPException(status_code=400, detail="X-Rec-Room-Profile is required.")
    return player_id


def _ensure_local_profile(request: Request, context) -> int:
    player_id = _local_profile_id(request)
    _BASE._ensure_existing_profile(context, player_id)
    return player_id


def _config_payload(context) -> dict[str, Any]:
    daily_objectives = [
        [{"type": int(obj["type"]), "score": int(obj["score"])} for obj in day]
        for day in _BASE.DAILY_OBJECTIVES
    ]
    return {
        "MessageOfTheDay": context.get_motd(API_VERSION),
        "MatchmakingParams": {
            "PreferFullRoomsFrequency": 0.5,
            "PreferEmptyRoomsFrequency": 0.0,
        },
        "DailyObjectives": daily_objectives,
        "ConfigTable": [],
    }


async def _handle_player_get_or_create(request: Request, context) -> Response:
    return await _BASE._handle_create_profile(request, "api/players/v1/create", context)


async def _handle_update_reputation_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    payload = await _BASE._parse_client_payload(request)
    delta = _BASE._int_field(payload, "reputationDelta", "ReputationDelta", default=0)
    state = player.get("state") or {}
    current = int(state.get("reputation") or _PLATFORM_BASE.DEFAULT_REPUTATION)
    state["reputation"] = current - delta
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], _PLATFORM_BASE.STATE_API_VERSION),
        )
    return Response(status_code=204)


async def _handle_verify_v2(request: Request) -> Response:
    payload = await _BASE._parse_client_payload(request)
    email = _BASE._str_field(payload, "Email", "email")
    message = "Verification email sent." if email else "Email address accepted."
    return JSONResponse({"Message": message})


async def _handle_objective_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return await _BASE._handle_complete_objective(request, f"api/players/v1/objective/{player_id}", context)


async def _handle_profile_image_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return await _BASE._handle_set_profile_image(request, f"api/images/v1/profile/{player_id}", context)


async def _handle_get_avatar_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return JSONResponse(_BASE._avatar_for_player(context, player_id))


async def _handle_set_avatar_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    payload = await _BASE._parse_client_payload(request)
    avatar = {
        "OutfitSelections": _BASE._str_field(payload, "OutfitSelections", "outfitSelections"),
        "HairColor": _BASE._str_field(payload, "HairColor", "hairColor"),
        "SkinColor": _BASE._str_field(payload, "SkinColor", "skinColor"),
    }
    _BASE._set_json_setting(context, _BASE._setting_key("avatar", player_id), avatar)
    return Response(status_code=204)


async def _handle_get_avatar_items_v3(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return await _BASE._handle_get_avatar_items(f"api/avatar/v2/items/{player_id}", context)


async def _handle_get_gifts_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return await _BASE._handle_get_gifts(f"api/avatar/v1/gifts/{player_id}", context)


async def _handle_create_gift_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return await _BASE._handle_create_gift(request, f"api/avatar/v1/gifts/create/{player_id}", context)


async def _handle_consume_gift_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    payload = await _BASE._parse_client_payload(request)
    gift_id = _BASE._int_field(payload, "Id", "id", default=0)
    unlocked_level = _BASE._int_field(payload, "UnlockedLevel", "unlockedLevel", default=0)
    gifts = _BASE._get_json_setting(context, _BASE._gift_key(player_id), [])
    consumed_gift = None
    remaining_gifts = []
    for gift in gifts:
        if isinstance(gift, dict) and int(gift.get("Id") or 0) == gift_id:
            consumed_gift = gift
        else:
            remaining_gifts.append(gift)
    _BASE._set_json_setting(context, _BASE._gift_key(player_id), remaining_gifts)
    if isinstance(consumed_gift, dict):
        item_desc = str(consumed_gift.get("AvatarItemDesc") or "")
        if item_desc:
            key = _BASE._setting_key("avatar_items", player_id)
            items = _BASE._get_json_setting(context, key, [])
            existing = {str(item.get("AvatarItemDesc") if isinstance(item, dict) else item) for item in items}
            if item_desc not in existing:
                items.append({"AvatarItemDesc": item_desc, "UnlockedLevel": unlocked_level})
                _BASE._set_json_setting(context, key, items)
    return Response(status_code=204)


async def _handle_get_settings_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return await _BASE._handle_get_settings(f"api/settings/v1/{player_id}", context)


async def _handle_set_setting_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    payload = await _BASE._parse_client_payload(request)
    key_name = _BASE._str_field(payload, "Key", "key")
    if not key_name:
        raise HTTPException(status_code=400, detail="Key is required.")
    key = _BASE._setting_key("player_settings", player_id)
    settings = _BASE._get_json_setting(context, key, {})
    settings[key_name] = _BASE._str_field(payload, "Value", "value")
    _BASE._set_json_setting(context, key, settings)
    return Response(status_code=204)


async def _handle_remove_setting_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    payload = await _BASE._parse_client_payload(request)
    key_name = _BASE._str_field(payload, "Key", "key")
    if not key_name:
        raise HTTPException(status_code=400, detail="Key is required.")
    key = _BASE._setting_key("player_settings", player_id)
    settings = _BASE._get_json_setting(context, key, {})
    settings.pop(key_name, None)
    _BASE._set_json_setting(context, key, settings)
    return Response(status_code=204)


async def _handle_presence_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return await _BASE._handle_update_presence(request, f"api/presence/v1/{player_id}", context)


async def _handle_get_relationships_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return await _BASE._handle_get_relationships(f"api/relationships/v1/get/{player_id}", context)


async def _handle_relationship_action_v2(request: Request, action: str, context) -> Response:
    local_id = _ensure_local_profile(request, context)
    remote_id = _BASE._int_field(dict(request.query_params), "id", default=0)
    if remote_id <= 0:
        raise HTTPException(status_code=400, detail="id is required.")
    _BASE._ensure_existing_profile(context, remote_id)
    relationships = _BASE._all_relationships(context)
    if action in {"addfriend", "acceptfriendrequest"}:
        _BASE._set_relationship(relationships, local_id, remote_id, _BASE.REL_FRIEND)
        _BASE._set_relationship(relationships, remote_id, local_id, _BASE.REL_FRIEND)
    elif action == "removefriend":
        _BASE._set_relationship(relationships, local_id, remote_id, _BASE.REL_NONE)
        _BASE._set_relationship(relationships, remote_id, local_id, _BASE.REL_NONE)
    elif action == "sendfriendrequest":
        _BASE._set_relationship(relationships, local_id, remote_id, _BASE.REL_FRIEND_REQUEST_SENT)
        _BASE._set_relationship(relationships, remote_id, local_id, _BASE.REL_FRIEND_REQUEST_RECEIVED)
    elif action == "blockplayer":
        remote_rel = _BASE._relationship_for(relationships, remote_id, local_id)
        local_rel = _BASE.REL_BLOCKED_MUTUAL if remote_rel == _BASE.REL_BLOCKED_LOCAL else _BASE.REL_BLOCKED_LOCAL
        remote_rel = _BASE.REL_BLOCKED_MUTUAL if remote_rel == _BASE.REL_BLOCKED_LOCAL else _BASE.REL_BLOCKED_REMOTE
        _BASE._set_relationship(relationships, local_id, remote_id, local_rel)
        _BASE._set_relationship(relationships, remote_id, local_id, remote_rel)
    elif action == "unblockplayer":
        _BASE._set_relationship(relationships, local_id, remote_id, _BASE.REL_NONE)
        if _BASE._relationship_for(relationships, remote_id, local_id) in {_BASE.REL_BLOCKED_REMOTE, _BASE.REL_BLOCKED_MUTUAL}:
            _BASE._set_relationship(relationships, remote_id, local_id, _BASE.REL_NONE)
    else:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    _BASE._save_relationships(context, relationships)
    return JSONResponse(_BASE._relationship_response(relationships, local_id, remote_id))


async def _handle_get_messages_v2(request: Request, context) -> Response:
    player_id = _ensure_local_profile(request, context)
    return await _BASE._handle_get_messages(f"api/messages/v1/get/{player_id}", context)


async def _handle_send_message_v2(request: Request, context) -> Response:
    from_id = _ensure_local_profile(request, context)
    payload = await _BASE._parse_client_payload(request)
    to_id = _BASE._int_field(payload, "ToPlayerId", "toPlayerId", default=0)
    if to_id <= 0:
        raise HTTPException(status_code=400, detail="ToPlayerId is required.")
    _BASE._ensure_existing_profile(context, to_id)
    messages = _BASE._all_messages(context)
    next_id = max([int(message.get("Id") or 0) for message in messages if isinstance(message, dict)] + [0]) + 1
    messages.append(
        {
            "Id": next_id,
            "FromPlayerId": from_id,
            "ToPlayerId": to_id,
            "SentTime": _BASE._dotnet_utc_ticks(),
            "Type": _BASE._int_field(payload, "Type", "type", default=0),
            "Data": _BASE._str_field(payload, "Data", "data"),
        }
    )
    _BASE._save_messages(context, messages)
    return Response(status_code=204)


async def _handle_delete_message_v2(request: Request, context) -> Response:
    return await _BASE._handle_delete_message(request, context)


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path in {"api/versioncheck/v1", "api/versioncheck/v1/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Version check method is not implemented.")
        return JSONResponse({})

    if path in {"api/config/v2", "api/config/v2/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Config method is not implemented.")
        return JSONResponse(_config_payload(context))

    if path.startswith("api/images/v1/profile/"):
        if method == "GET":
            return await _BASE._handle_get_profile_image(request, route_path, context)
        raise HTTPException(status_code=501, detail="Profile image method is not implemented.")
    if path in {"api/images/v2/profile", "api/images/v2/profile/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Profile image upload method is not implemented.")
        return await _handle_profile_image_v2(request, context)

    if method == "GET" and path in {"api/players/v2", "api/players/v2/"}:
        return await _BASE._handle_get_profile_by_platform(request, context)
    if path in {"api/players/v1/getorcreate", "api/players/v1/getorcreate/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player get-or-create method is not implemented.")
        return await _handle_player_get_or_create(request, context)
    if path in {"api/players/v2/updatereputation", "api/players/v2/updatereputation/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player reputation method is not implemented.")
        return await _handle_update_reputation_v2(request, context)
    if path in {"api/players/v2/verify", "api/players/v2/verify/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player verify method is not implemented.")
        return await _handle_verify_v2(request)
    if path in {"api/players/v2/objective", "api/players/v2/objective/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player objective method is not implemented.")
        return await _handle_objective_v2(request, context)
    if path in {"api/players/v1/score", "api/players/v1/score/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player score method is not implemented.")
        return Response(status_code=204)

    if path in {"api/avatar/v2", "api/avatar/v2/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Avatar method is not implemented.")
        return await _handle_get_avatar_v2(request, context)
    if path in {"api/avatar/v2/set", "api/avatar/v2/set/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Avatar set method is not implemented.")
        return await _handle_set_avatar_v2(request, context)
    if path in {"api/avatar/v3/items", "api/avatar/v3/items/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Avatar items method is not implemented.")
        return await _handle_get_avatar_items_v3(request, context)
    if path in {"api/avatar/v2/gifts", "api/avatar/v2/gifts/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Avatar gifts method is not implemented.")
        return await _handle_get_gifts_v2(request, context)
    if path in {"api/avatar/v2/gifts/create", "api/avatar/v2/gifts/create/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Avatar gift create method is not implemented.")
        return await _handle_create_gift_v2(request, context)
    if path in {"api/avatar/v2/gifts/consume", "api/avatar/v2/gifts/consume/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Avatar gift consume method is not implemented.")
        return await _handle_consume_gift_v2(request, context)

    if path in {"api/settings/v2", "api/settings/v2/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Settings method is not implemented.")
        return await _handle_get_settings_v2(request, context)
    if path in {"api/settings/v2/set", "api/settings/v2/set/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Settings set method is not implemented.")
        return await _handle_set_setting_v2(request, context)
    if path in {"api/settings/v2/remove", "api/settings/v2/remove/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Settings remove method is not implemented.")
        return await _handle_remove_setting_v2(request, context)

    if path in {"api/presence/v2", "api/presence/v2/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Presence update method is not implemented.")
        return await _handle_presence_v2(request, context)

    if path in {"api/relationships/v2/get", "api/relationships/v2/get/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Relationships method is not implemented.")
        return await _handle_get_relationships_v2(request, context)
    for action in ("addfriend", "removefriend", "sendfriendrequest", "acceptfriendrequest", "blockplayer", "unblockplayer"):
        if path in {f"api/relationships/v2/{action}", f"api/relationships/v2/{action}/"}:
            if method != "GET":
                raise HTTPException(status_code=501, detail="Relationship action method is not implemented.")
            return await _handle_relationship_action_v2(request, action, context)

    if path in {"api/messages/v2/get", "api/messages/v2/get/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Messages method is not implemented.")
        return await _handle_get_messages_v2(request, context)
    if path in {"api/messages/v2/send", "api/messages/v2/send/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Message send method is not implemented.")
        return await _handle_send_message_v2(request, context)
    if path in {"api/messages/v2/delete", "api/messages/v2/delete/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Message delete method is not implemented.")
        return await _handle_delete_message_v2(request, context)

    if path in {"api/analytics/v1/session/event", "api/analytics/v1/session/event/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Analytics event method is not implemented.")
        return Response(status_code=204)

    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    path = _clean_route_path(route_path).casefold()
    if path not in {"api/notification/v2", "api/notification/v2/"}:
        await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
        return
    await websocket.accept()
    try:
        await websocket.receive_text()
        session_id = int(time.time() * 1000)
        await websocket.send_text(json.dumps({"SessionId": session_id}))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        return
