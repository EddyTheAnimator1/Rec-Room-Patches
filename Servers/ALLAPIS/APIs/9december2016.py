"""9 December 2016 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 8224493981844824938:
- Startup now probes GET api/versioncheck/v1, downloads GET api/config/v2,
  then creates/loads the local profile through POST api/players/v1/getorcreate.
- Local-player endpoints accept X-Rec-Room-Profile when present. The localhost
  bridge can inject that header after login for clients that omit it.
- Avatar, image upload, settings, relationships, messages, and presence moved
  to v2/v3 routes while keeping the same underlying data shapes.
- Push notification WebSocket moved to api/notification/v2 and expects a JSON
  handshake response with a positive SessionId.
"""

from __future__ import annotations

import importlib.util
import json
import re
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

_RELATIONSHIP_CHANGED = 1
_MESSAGE_RECEIVED = 2
_MESSAGE_DELETED = 3
_SUBSCRIPTION_UPDATE_PRESENCE = 10
_NOTIFICATION_CLIENTS_BY_PLAYER: dict[int, set[WebSocket]] = {}
_NOTIFICATION_PLAYER_BY_CLIENT: dict[WebSocket, int] = {}


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _fallback_profile_id(context) -> int:
    return 0


def _local_profile_id(request: Request | WebSocket, context=None) -> int:
    raw_id = request.headers.get("X-Rec-Room-Profile") or request.headers.get("x-rec-room-profile")
    try:
        player_id = int(raw_id or 0)
    except Exception:
        player_id = 0
    if player_id > 0:
        return player_id
    return _fallback_profile_id(context) if context is not None else 1


def _ensure_local_profile(request: Request | WebSocket, context) -> int:
    player_id = _local_profile_id(request, context)
    _BASE._ensure_existing_profile(context, player_id)
    return player_id


def _json_object_from_text(raw_message: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_message)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_json_object(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        return _json_object_from_text(payload)
    return {}


def _notification_param(payload: dict[str, Any]) -> Any:
    for name in ("param", "Param", "PARAM", "params", "Params", "body", "Body", "data", "Data"):
        if name in payload:
            return payload[name]
    return None


def _coerce_subscription_ids(payload: Any) -> list[int]:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return []
    if isinstance(payload, dict):
        single_id = payload.get("PlayerId") or payload.get("playerId") or payload.get("Id") or payload.get("id")
        payload = (
            payload.get("PlayerIds")
            or payload.get("playerIds")
            or payload.get("PlayerIDs")
            or payload.get("Ids")
            or payload.get("ids")
            or ([single_id] if single_id is not None else [])
        )
    if not isinstance(payload, list):
        return []

    player_ids: list[int] = []
    for value in payload:
        try:
            player_id = int(value)
        except Exception:
            continue
        if player_id > 0:
            player_ids.append(player_id)
    return player_ids


def _subscription_key(player_id: int) -> str:
    owner = player_id if player_id > 0 else "anonymous"
    return _BASE._setting_key("player_subscriptions", owner)


def _stored_subscription_ids(context, player_id: int) -> set[int]:
    subscribed: set[int] = set()
    for value in _BASE._get_json_setting(context, _subscription_key(player_id), []):
        try:
            stored_id = int(value)
        except Exception:
            continue
        if stored_id > 0:
            subscribed.add(stored_id)
    return subscribed


def _handle_player_subscription_notification(api: str, param: Any, player_id: int, context) -> bool:
    match = re.fullmatch(r"(?:api/)?playersubscriptions/v1(?:/(init|add|remove))?/?", api, flags=re.IGNORECASE)
    if not match:
        return False

    raw_action = match.group(1)
    if raw_action is None and isinstance(param, dict):
        raw_action = str(param.get("Action") or param.get("action") or param.get("Operation") or param.get("operation") or "")
    action = str(raw_action or "").strip("/").casefold()
    if action not in {"init", "add", "remove"}:
        return False
    requested_ids = set(_coerce_subscription_ids(param))
    subscribed = _stored_subscription_ids(context, player_id)
    if action == "init":
        subscribed = requested_ids
    elif action == "add":
        subscribed.update(requested_ids)
    elif action == "remove":
        subscribed.difference_update(requested_ids)
    _BASE._set_json_setting(context, _subscription_key(player_id), sorted(subscribed))
    return True


def _notification_payload(event_id: int, message: dict[str, Any]) -> str:
    return json.dumps({"Id": event_id, "Msg": message}, separators=(",", ":"))


def _message_notification_payload(message: dict[str, Any]) -> dict[str, Any]:
    return {
        "Id": int(message.get("Id") or 0),
        "FromPlayerId": int(message.get("FromPlayerId") or 0),
        "SentTime": int(message.get("SentTime") or _BASE._dotnet_utc_ticks()),
        "Type": int(message.get("Type") or 0),
        "Data": str(message.get("Data") or ""),
    }


def _registered_player_ids() -> list[int]:
    return sorted(player_id for player_id, clients in _NOTIFICATION_CLIENTS_BY_PLAYER.items() if clients)


async def _register_notification_client(websocket: WebSocket, player_id: int) -> None:
    _NOTIFICATION_PLAYER_BY_CLIENT[websocket] = player_id
    _NOTIFICATION_CLIENTS_BY_PLAYER.setdefault(player_id, set()).add(websocket)


async def _unregister_notification_client(websocket: WebSocket, context) -> None:
    player_id = _NOTIFICATION_PLAYER_BY_CLIENT.pop(websocket, None)
    if player_id is None:
        return
    clients = _NOTIFICATION_CLIENTS_BY_PLAYER.get(player_id)
    if clients is not None:
        clients.discard(websocket)
        if clients:
            return
        _NOTIFICATION_CLIENTS_BY_PLAYER.pop(player_id, None)
    presence = _presence_payload_from_client(context, player_id, {"IsOnline": False}, default_online=False)
    _save_presence(context, player_id, presence)
    await _broadcast_notification(_registered_player_ids(), _SUBSCRIPTION_UPDATE_PRESENCE, presence)


async def _send_notification(player_id: int, event_id: int, message: dict[str, Any]) -> None:
    clients = list(_NOTIFICATION_CLIENTS_BY_PLAYER.get(player_id, set()))
    if not clients:
        return
    encoded = _notification_payload(event_id, message)
    stale: list[WebSocket] = []
    for client in clients:
        try:
            await client.send_text(encoded)
        except Exception:
            stale.append(client)
    for client in stale:
        _NOTIFICATION_PLAYER_BY_CLIENT.pop(client, None)
        clients_for_player = _NOTIFICATION_CLIENTS_BY_PLAYER.get(player_id)
        if clients_for_player is not None:
            clients_for_player.discard(client)
            if not clients_for_player:
                _NOTIFICATION_CLIENTS_BY_PLAYER.pop(player_id, None)


async def _broadcast_notification(player_ids: list[int], event_id: int, message: dict[str, Any]) -> None:
    for player_id in player_ids:
        await _send_notification(player_id, event_id, message)


def _presence_payload_from_client(context, player_id: int, payload: dict[str, Any], *, default_online: bool = True) -> dict[str, Any]:
    current = _BASE._presence_for_player(context, player_id)
    presence = _BASE._default_presence(player_id)
    presence.update(current)
    presence.update(
        {
            "PlayerId": player_id,
            "IsOnline": _BASE._bool_field(payload, "IsOnline", "isOnline", default=default_online),
            "GameSessionId": _BASE._str_field(payload, "GameSessionId", "gameSessionId", default=str(current.get("GameSessionId") or "")),
            "AppVersion": _BASE._str_field(payload, "AppVersion", "appVersion", default=str(current.get("AppVersion") or "")),
            "Activity": _BASE._str_field(payload, "Activity", "activity", default=str(current.get("Activity") or "")),
            "Private": _BASE._bool_field(payload, "Private", "private", default=bool(current.get("Private"))),
            "AvailableSpace": _BASE._int_field(
                payload,
                "AvailableSpace",
                "availableSpace",
                default=int(current.get("AvailableSpace") or 0),
            ),
            "GameInProgress": _BASE._bool_field(payload, "GameInProgress", "gameInProgress", default=bool(current.get("GameInProgress"))),
            "LastUpdateTime": _BASE._dotnet_utc_ticks(),
        }
    )
    return presence


def _save_presence(context, player_id: int, presence: dict[str, Any]) -> None:
    _BASE._set_json_setting(context, _BASE._setting_key("presence", player_id), presence)


async def _publish_presence(context, player_id: int, payload: dict[str, Any], *, default_online: bool = True) -> None:
    presence = _presence_payload_from_client(context, player_id, payload, default_online=default_online)
    _save_presence(context, player_id, presence)
    await _broadcast_notification(_registered_player_ids(), _SUBSCRIPTION_UPDATE_PRESENCE, presence)


async def _handle_notification_client_message(raw_message: str, player_id: int, context) -> None:
    payload = _json_object_from_text(raw_message)
    api = str(payload.get("api") or payload.get("Api") or payload.get("API") or "").strip("/")
    param = _notification_param(payload)
    if api.casefold() != "presence/v1":
        if param is None:
            param = payload
        _handle_player_subscription_notification(api, param, player_id, context)
        return
    await _publish_presence(context, player_id, _coerce_json_object(param))


async def _notify_relationship_changed(relationships: dict[str, dict[str, int]], local_id: int, remote_id: int, context) -> None:
    flags = _BASE._all_relationship_flags(context)
    await _send_notification(local_id, _RELATIONSHIP_CHANGED, _BASE._relationship_response(relationships, local_id, remote_id, flags))
    if remote_id != local_id:
        await _send_notification(remote_id, _RELATIONSHIP_CHANGED, _BASE._relationship_response(relationships, remote_id, local_id, flags))


async def _notify_message_received(message: dict[str, Any]) -> None:
    await _send_notification(int(message.get("ToPlayerId") or 0), _MESSAGE_RECEIVED, _message_notification_payload(message))


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
    response = await _BASE._handle_update_presence(request, f"api/presence/v1/{player_id}", context)
    await _broadcast_notification(_registered_player_ids(), _SUBSCRIPTION_UPDATE_PRESENCE, _BASE._presence_for_player(context, player_id))
    return response


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
    await _notify_relationship_changed(relationships, local_id, remote_id, context)
    return JSONResponse(_BASE._relationship_response(relationships, local_id, remote_id, _BASE._all_relationship_flags(context)))


async def _handle_relationship_flag_v1(request: Request, action: str, context) -> Response:
    local_id = _ensure_local_profile(request, context)
    payload = await _BASE._parse_client_payload(request)
    remote_id = _BASE._int_field(payload, "PlayerId", "playerId", "ToPlayerId", "toPlayerId", default=0)
    if remote_id <= 0:
        remote_id = _BASE._int_field(dict(request.query_params), "id", "id2", "playerId", default=0)
    if remote_id <= 0:
        raise HTTPException(status_code=400, detail="PlayerId is required.")
    _BASE._ensure_existing_profile(context, remote_id)
    flag_name = "Mute" if action in {"mute", "unmute"} else "Ignore"
    enabled = action in {"mute", "ignore"}
    flags = _BASE._all_relationship_flags(context)
    _BASE._set_relationship_flag(flags, local_id, remote_id, flag_name, enabled)
    _BASE._save_relationship_flags(context, flags)
    relationships = _BASE._all_relationships(context)
    await _send_notification(local_id, _RELATIONSHIP_CHANGED, _BASE._relationship_response(relationships, local_id, remote_id, flags))
    return Response(status_code=204)


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
    message = {
        "Id": next_id,
        "FromPlayerId": from_id,
        "ToPlayerId": to_id,
        "SentTime": _BASE._dotnet_utc_ticks(),
        "Type": _BASE._int_field(payload, "Type", "type", default=0),
        "Data": _BASE._str_field(payload, "Data", "data"),
    }
    messages.append(message)
    _BASE._save_messages(context, messages)
    await _notify_message_received(message)
    return Response(status_code=204)


async def _handle_delete_message_v2(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    message_id = _BASE._int_field(payload, "Id", "id", default=0)
    response = await _BASE._handle_delete_message(request, context)
    if message_id > 0:
        await _send_notification(_ensure_local_profile(request, context), _MESSAGE_DELETED, {"Id": message_id})
    return response


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
    for action in ("mute", "unmute", "ignore", "unignore"):
        if path in {f"api/relationships/v1/{action}", f"api/relationships/v1/{action}/"}:
            if method != "POST":
                raise HTTPException(status_code=501, detail="Relationship flag method is not implemented.")
            return await _handle_relationship_flag_v1(request, action, context)
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
        handshake = _json_object_from_text(await websocket.receive_text())
        player_id = _ensure_local_profile(websocket, context)
        await _register_notification_client(websocket, player_id)
        session_id = int(time.time() * 1000)
        await websocket.send_text(json.dumps({"SessionId": session_id}))
        await _publish_presence(context, player_id, handshake)
        while True:
            await _handle_notification_client_message(await websocket.receive_text(), player_id, context)
    except WebSocketDisconnect:
        await _unregister_notification_client(websocket, context)
    except Exception:
        await _unregister_notification_client(websocket, context)
        raise
