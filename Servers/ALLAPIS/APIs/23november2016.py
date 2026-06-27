"""23 November 2016 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 2999110564380282122:
- GET  api/players/v1/?p=<platform>&id=<platform id>
- GET  api/players/v1/<Id>
- POST api/players/v1/list with a JSON list of profile ids
- POST api/players/v1/create with form fields Platform, PlatformId, and Name
- POST api/players/v1/updateReputation/<Id> with form field reputation
- POST api/players/v1/objective/<Id> with objectiveType, additionalXp, and inParty
- POST api/players/v1/verify/<Id> with form field email
- GET  api/avatar/v1/<Id>
- POST api/avatar/v1/set with Avatar JSON
- GET  api/avatar/v1/gifts/<Id>
- POST api/avatar/v1/gifts/create/<Id>
- POST api/avatar/v1/gifts/consume
- GET  api/avatar/v2/items/<Id>
- POST api/avatar/v1/items/create
- GET  api/settings/v1/<Id>
- POST api/settings/v1/set
- POST api/settings/v1/remove
- GET  api/presence/v1/<Id>
- POST api/presence/v1/<Id>
- POST api/presence/v1/list
- GET  api/relationships/v1/get/<Id>
- GET  api/relationships/v1/addfriend|removefriend|sendfriendrequest|acceptfriendrequest|blockplayer|unblockplayer
- GET  api/messages/v1/get/<Id>
- POST api/messages/v1/send
- POST api/messages/v1/delete
- GET  api/gamesessions/v1/
- GET  api/gamesessions/v1/<Id>
- WebSocket api/notification/v1

This build is the first November build in this restoration set with RecNet
social/presence/messaging and push notification startup flow. Unsupported
methods stay 501 and unknown endpoints stay 404.
"""

from __future__ import annotations

import importlib.util
import json
import mimetypes
import re
import time
from datetime import datetime, timezone
from email.parser import BytesParser
from email.policy import default as email_default_policy
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

from fastapi import HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, PlainTextResponse, Response

API_VERSION = "23november2016"
PROFILE_IMAGE_PURPOSE = "shared.profile_image"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Wed, 23 Nov 2016 01:26:08 GMT"
DEFAULT_PROFILE_IMAGE_BYTES = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4"
    b"\x89\x00\x00\x00\rIDATx\x9cc\xf8\xff\xff?\x00\x05"
    b"\xfe\x02\xfeA\xe2 \x9a\x00\x00\x00\x00IEND\xaeB`\x82"
)

DAILY_OBJECTIVES = [
    [
        {"type": 301, "score": 1, "xp": 100},
        {"type": 500, "score": 1, "xp": 100},
        {"type": 801, "score": 1, "xp": 100},
    ],
    [
        {"type": 201, "score": 1, "xp": 100},
        {"type": 400, "score": 1, "xp": 100},
        {"type": 100, "score": 1, "xp": 100},
    ],
    [
        {"type": 601, "score": 1, "xp": 100},
        {"type": 701, "score": 1, "xp": 100},
        {"type": 301, "score": 1, "xp": 100},
    ],
    [
        {"type": 801, "score": 1, "xp": 100},
        {"type": 201, "score": 1, "xp": 100},
        {"type": 500, "score": 1, "xp": 100},
    ],
    [
        {"type": 100, "score": 1, "xp": 100},
        {"type": 400, "score": 1, "xp": 100},
        {"type": 301, "score": 1, "xp": 100},
    ],
    [
        {"type": 500, "score": 1, "xp": 100},
        {"type": 801, "score": 1, "xp": 100},
        {"type": 201, "score": 1, "xp": 100},
    ],
    [
        {"type": 301, "score": 1, "xp": 100},
        {"type": 400, "score": 1, "xp": 100},
        {"type": 100, "score": 1, "xp": 100},
    ],
]

DEFAULT_AVATAR = {"OutfitSelections": "", "HairColor": "", "SkinColor": ""}
DEFAULT_XP_REWARD = 100
XP_PER_LEVEL = 1000
DOTNET_TICKS_AT_UNIX_EPOCH = 621355968000000000

REL_NONE = 0
REL_FRIEND_REQUEST_SENT = 1
REL_FRIEND_REQUEST_RECEIVED = 2
REL_FRIEND = 3
REL_BLOCKED_LOCAL = 4
REL_BLOCKED_REMOTE = 5
REL_BLOCKED_MUTUAL = 6


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _dotnet_utc_ticks() -> int:
    return DOTNET_TICKS_AT_UNIX_EPOCH + int(time.time() * 10_000_000)


def _load_response_json(response: Response) -> Any:
    body = getattr(response, "body", b"")
    if isinstance(body, str):
        body = body.encode("utf-8")
    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        return None


def _set_api_version(module) -> None:
    seen: set[int] = set()
    stack = [module]
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if hasattr(current, "API_VERSION"):
            current.API_VERSION = API_VERSION
        for attr in ("_BASE", "_PLATFORM_BASE"):
            child = getattr(current, attr, None)
            if child is not None:
                stack.append(child)


def _load_base_adapter():
    module_path = Path(__file__).with_name("16november2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_16november2016_shared_for_23november2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 16november2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _set_api_version(module)
    return module


_BASE = _load_base_adapter()


def _find_platform_base(module):
    seen: set[int] = set()
    stack = [module]
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if hasattr(current, "_find_player_by_platform") and hasattr(current, "_serialize_player_for_client"):
            return current
        for attr in ("_PLATFORM_BASE", "_BASE"):
            child = getattr(current, attr, None)
            if child is not None:
                stack.append(child)
    raise RuntimeError("Could not find shared platform player adapter.")


_PLATFORM_BASE = _find_platform_base(_BASE)


def _int_field(payload: dict[str, Any], *names: str, default: int = 0) -> int:
    for name in names:
        if name in payload and payload[name] is not None:
            try:
                return int(payload[name])
            except Exception:
                return default
    return default


def _str_field(payload: dict[str, Any], *names: str, default: str = "") -> str:
    for name in names:
        if name in payload and payload[name] is not None:
            return str(payload[name]).strip()
    return default


def _bool_field(payload: dict[str, Any], *names: str, default: bool = False) -> bool:
    for name in names:
        if name in payload and payload[name] is not None:
            value = payload[name]
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(value)
            if isinstance(value, str):
                return value.strip().casefold() in {"1", "true", "yes", "on"}
            return bool(value)
    return default


def _parse_multipart_fields(body: bytes, content_type: str) -> dict[str, str]:
    match = re.search(r"boundary=([^;]+)", content_type, flags=re.IGNORECASE)
    if not match:
        return {}
    boundary = match.group(1).strip().strip('"').encode("utf-8")
    delimiter = b"--" + boundary
    fields: dict[str, str] = {}
    for part in body.split(delimiter):
        part = part.strip(b"\r\n")
        if not part or part == b"--" or b"\r\n\r\n" not in part:
            continue
        raw_headers, raw_value = part.split(b"\r\n\r\n", 1)
        header_text = raw_headers.decode("utf-8", errors="replace")
        name_match = re.search(r'name="([^"]+)"', header_text)
        if not name_match:
            continue
        value = raw_value.rstrip(b"\r\n-").decode("utf-8", errors="replace")
        fields[name_match.group(1)] = value
    return fields


async def _parse_body_any(request: Request) -> Any:
    body = await request.body()
    if not body:
        return {}
    content_type = str(request.headers.get("content-type") or "")
    lowered = content_type.casefold()
    if "multipart/form-data" in lowered:
        return _parse_multipart_fields(body, content_type)
    if "json" in lowered:
        try:
            return json.loads(body.decode("utf-8"))
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Invalid JSON payload.") from exc
    if "x-www-form-urlencoded" in lowered:
        parsed = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
        return {key: values[-1] if values else "" for key, values in parsed.items()}
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        parsed = parse_qs(text, keep_blank_values=True)
        return {key: values[-1] if values else "" for key, values in parsed.items()}


async def _parse_client_payload(request: Request) -> dict[str, Any]:
    payload = await _parse_body_any(request)
    return payload if isinstance(payload, dict) else {}


def _legacy_id_from_image_route(route_path: str) -> int | None:
    match = re.fullmatch(r"api/images/v1/profile/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def _image_asset_for_player(context, player_id: str):
    with context.db.connection() as conn:
        player = conn.execute("SELECT profile_picture_asset_id FROM players WHERE player_id = ?", (player_id,)).fetchone()
        asset_id = player["profile_picture_asset_id"] if player else None
        if asset_id:
            asset = conn.execute("SELECT * FROM data_assets WHERE asset_id = ?", (asset_id,)).fetchone()
            if asset:
                return asset
        return conn.execute(
            """
            SELECT *
            FROM data_assets
            WHERE owner_player_id = ?
              AND purpose = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (player_id, PROFILE_IMAGE_PURPOSE),
        ).fetchone()


def _detect_image_type(content: bytes, fallback_mime: str = "") -> tuple[str, str]:
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", ".png"
    if content.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", ".jpg"
    fallback_mime = fallback_mime.lower()
    fallback_ext = mimetypes.guess_extension(fallback_mime) or ""
    if fallback_mime in {"image/png", "image/jpeg"} and fallback_ext.lower() in {".png", ".jpg", ".jpeg"}:
        return fallback_mime, fallback_ext.lower()
    raise HTTPException(status_code=400, detail="image must be PNG or JPEG.")


def _parse_multipart_image(content_type: str, body: bytes) -> tuple[bytes, str]:
    if "multipart/form-data" not in content_type.casefold():
        mime_type, _ = _detect_image_type(body, content_type.split(";", 1)[0].strip())
        return body, mime_type
    if "boundary=" not in content_type.casefold():
        raise HTTPException(status_code=400, detail="Multipart boundary is required.")

    message = BytesParser(policy=email_default_policy).parsebytes(
        b"Content-Type: " + content_type.encode("utf-8") + b"\r\n\r\n" + body
    )
    for part in message.iter_parts():
        disposition = part.get_content_disposition()
        field_name = part.get_param("name", header="content-disposition")
        if disposition == "form-data" and field_name == "image":
            content = part.get_payload(decode=True) or b""
            return content, str(part.get_content_type() or "")
    raise HTTPException(status_code=400, detail="image form field is required.")


def _http_date_from_created_at(created_at: Any) -> str:
    value = str(created_at or "").strip()
    try:
        stamp = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    return format_datetime(stamp, usegmt=True)


def _same_http_date(left: str, right: str) -> bool:
    if not left or not right:
        return False
    try:
        return parsedate_to_datetime(left) == parsedate_to_datetime(right)
    except Exception:
        return left.strip() == right.strip()


def _profile_image_response(
    request: Request,
    *,
    content: bytes,
    media_type: str,
    last_modified: str,
) -> Response:
    headers = {"Last-Modified": last_modified}
    if _same_http_date(str(request.headers.get("if-modified-since") or ""), last_modified):
        return Response(status_code=304, headers=headers)
    return Response(content=content, media_type=media_type, headers=headers)


async def _handle_get_profile_image(request: Request, route_path: str, context) -> Response:
    legacy_id = _legacy_id_from_image_route(route_path)
    if legacy_id is None:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
    if player is None:
        return _profile_image_response(
            request,
            content=DEFAULT_PROFILE_IMAGE_BYTES,
            media_type="image/png",
            last_modified=DEFAULT_PROFILE_IMAGE_LAST_MODIFIED,
        )
    context.assert_player_not_banned(player["player_id"])
    asset = _image_asset_for_player(context, player["player_id"])
    if asset is None:
        return _profile_image_response(
            request,
            content=DEFAULT_PROFILE_IMAGE_BYTES,
            media_type="image/png",
            last_modified=DEFAULT_PROFILE_IMAGE_LAST_MODIFIED,
        )
    image_path = (context.data_dir / asset["relative_path"]).resolve()
    data_dir = context.data_dir.resolve()
    if data_dir not in image_path.parents or not image_path.is_file():
        return _profile_image_response(
            request,
            content=DEFAULT_PROFILE_IMAGE_BYTES,
            media_type="image/png",
            last_modified=DEFAULT_PROFILE_IMAGE_LAST_MODIFIED,
        )
    return _profile_image_response(
        request,
        content=image_path.read_bytes(),
        media_type=str(asset["mime_type"] or "application/octet-stream"),
        last_modified=_http_date_from_created_at(asset["created_at"]),
    )


async def _handle_set_profile_image(request: Request, route_path: str, context) -> Response:
    legacy_id = _legacy_id_from_image_route(route_path)
    if legacy_id is None:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])

    body = await request.body()
    content, declared_mime_type = _parse_multipart_image(str(request.headers.get("content-type") or ""), body)
    if not content:
        raise HTTPException(status_code=400, detail="image form field is empty.")
    mime_type, file_ext = _detect_image_type(content, declared_mime_type)
    try:
        asset = context.save_image_bytes(
            owner_player_id=player["player_id"],
            content=content,
            file_ext=file_ext,
            mime_type=mime_type,
            purpose=PROFILE_IMAGE_PURPOSE,
            metadata={"legacy_player_id": legacy_id},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET profile_picture_asset_id = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
            """,
            (asset["asset_id"], player["player_id"]),
        )
    return Response(status_code=204)


def _display_name_for_profile(player: dict[str, Any]) -> str:
    state = player.get("state") or {}
    return str(player.get("display_name") or state.get("name") or player.get("username") or "Player")


def _serialize_profile_for_recnet(player: dict[str, Any]) -> dict[str, Any]:
    state = player.get("state") or {}
    display_name = _display_name_for_profile(player)
    return {
        "Id": int(state.get("legacy_player_id") or 0),
        "Username": display_name,
        "DisplayName": display_name,
        "XP": int(player.get("canonical_xp") or 0),
        "XpRequiredToLevelUp": XP_PER_LEVEL,
        "Level": max(1, int(player.get("canonical_level") or 1)),
        "Reputation": int(state.get("reputation") or _PLATFORM_BASE.DEFAULT_REPUTATION),
        "Verified": bool(player.get("verified")),
    }


def _json_profile_response(player: dict[str, Any], *, status_code: int = 200) -> Response:
    return JSONResponse(_serialize_profile_for_recnet(player), status_code=status_code)


async def _handle_get_profile_by_platform(request: Request, context) -> Response:
    response = await _BASE.handle_http(request=request, route_path="api/players/v1/", context=context)
    payload = _load_response_json(response)
    if payload is None:
        return response
    if isinstance(payload, dict) and "Id" in payload:
        player = _PLATFORM_BASE._find_player_by_legacy_id(context, int(payload.get("Id") or 0))
        if player is not None:
            return _json_profile_response(player, status_code=getattr(response, "status_code", 200))
    return response


async def _handle_get_profile_by_id(route_path: str, context) -> Response:
    match = re.fullmatch(r"api/players/v1/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, int(match.group(1)))
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    return _json_profile_response(player)


async def _handle_create_profile(request: Request, route_path: str, context) -> Response:
    response = await _BASE.handle_http(request=request, route_path=route_path, context=context)
    payload = _load_response_json(response)
    if not isinstance(payload, dict) or "Id" not in payload:
        return response
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, int(payload["Id"]))
    if player is None:
        return response
    return _json_profile_response(player, status_code=getattr(response, "status_code", 200))


async def _handle_profile_list(request: Request, context) -> Response:
    payload = await _parse_body_any(request)
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="Profile list payload must be a JSON list.")
    profiles: list[dict[str, Any]] = []
    for raw_id in payload:
        try:
            legacy_id = int(raw_id)
        except Exception:
            continue
        player = _PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
        if player is not None:
            context.assert_player_not_banned(player["player_id"])
            profiles.append(_serialize_profile_for_recnet(player))
    return JSONResponse(profiles)


async def _handle_update_reputation(request: Request, route_path: str, context) -> Response:
    match = re.fullmatch(r"api/players/v1/updatereputation/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    legacy_id = int(match.group(1))
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    payload = await _parse_client_payload(request)
    state = player.get("state") or {}
    state["reputation"] = _int_field(payload, "reputation", "Reputation", default=int(state.get("reputation") or 0))
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


async def _handle_complete_objective(request: Request, route_path: str, context) -> Response:
    match = re.fullmatch(r"api/players/v1/objective/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    legacy_id = int(match.group(1))
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    payload = await _parse_client_payload(request)
    additional_xp = max(0, _int_field(payload, "additionalXp", "AdditionalXp", default=0))
    delta_xp = DEFAULT_XP_REWARD + additional_xp
    current_xp_total = max(0, int(player.get("canonical_xp") or 0)) + delta_xp
    current_level = max(1, current_xp_total // XP_PER_LEVEL + 1)
    current_xp = current_xp_total % XP_PER_LEVEL
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET canonical_xp = ?,
                canonical_level = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND is_coach = 0
            """,
            (current_xp, current_level, player["player_id"]),
        )
    return JSONResponse(
        {
            "deltaXp": delta_xp,
            "currentLevel": current_level,
            "currentXp": current_xp,
            "xpRequiredToLevelUp": XP_PER_LEVEL,
        }
    )


def _setting_key(kind: str, player_id: int | str = "global") -> str:
    return f"{API_VERSION}.{kind}.{player_id}"


def _get_json_setting(context, key: str, default: Any) -> Any:
    with context.db.connection() as conn:
        row = conn.execute("SELECT value_json FROM server_settings WHERE key = ?", (key,)).fetchone()
    if row is None:
        return default
    try:
        value = json.loads(row["value_json"])
    except Exception:
        return default
    return value if isinstance(value, type(default)) else default


def _set_json_setting(context, key: str, value: Any) -> None:
    with context.db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO server_settings(key, value_json, created_at, updated_at)
            VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            ON CONFLICT(key) DO UPDATE
            SET value_json = excluded.value_json,
                updated_at = excluded.updated_at
            """,
            (key, json.dumps(value, sort_keys=True)),
        )


def _ensure_existing_profile(context, player_id: int) -> None:
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])


def _avatar_for_player(context, player_id: int) -> dict[str, str]:
    avatar = _get_json_setting(context, _setting_key("avatar", player_id), dict(DEFAULT_AVATAR))
    return {
        "OutfitSelections": str(avatar.get("OutfitSelections") or ""),
        "HairColor": str(avatar.get("HairColor") or ""),
        "SkinColor": str(avatar.get("SkinColor") or ""),
    }


async def _handle_get_avatar(route_path: str, context) -> Response:
    match = re.fullmatch(r"api/avatar/v1/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player_id = int(match.group(1))
    _ensure_existing_profile(context, player_id)
    return JSONResponse(_avatar_for_player(context, player_id))


async def _handle_set_avatar(request: Request, context) -> Response:
    payload = await _parse_client_payload(request)
    player_id = _int_field(payload, "PlayerId", "playerId", "player_id", default=0)
    if player_id <= 0:
        raise HTTPException(status_code=400, detail="PlayerId is required.")
    _ensure_existing_profile(context, player_id)
    avatar = {
        "OutfitSelections": _str_field(payload, "OutfitSelections", "outfitSelections"),
        "HairColor": _str_field(payload, "HairColor", "hairColor"),
        "SkinColor": _str_field(payload, "SkinColor", "skinColor"),
    }
    _set_json_setting(context, _setting_key("avatar", player_id), avatar)
    return Response(status_code=204)


async def _handle_get_avatar_items(route_path: str, context) -> Response:
    match = re.fullmatch(r"api/avatar/v2/items/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player_id = int(match.group(1))
    _ensure_existing_profile(context, player_id)
    items = _get_json_setting(context, _setting_key("avatar_items", player_id), [])
    normalized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            normalized.append(
                {
                    "AvatarItemDesc": str(item.get("AvatarItemDesc") or ""),
                    "UnlockedLevel": int(item.get("UnlockedLevel") or 0),
                }
            )
        else:
            normalized.append({"AvatarItemDesc": str(item), "UnlockedLevel": 0})
    return JSONResponse(normalized)


async def _handle_create_avatar_item(request: Request, context) -> Response:
    payload = await _parse_client_payload(request)
    player_id = _int_field(payload, "PlayerId", "playerId", "player_id", default=0)
    if player_id <= 0:
        raise HTTPException(status_code=400, detail="PlayerId is required.")
    _ensure_existing_profile(context, player_id)
    item_desc = _str_field(payload, "AvatarItemDesc", "avatarItemDesc", "avatar_item_desc")
    unlocked_level = _int_field(payload, "UnlockedLevel", "unlockedLevel", default=0)
    if item_desc:
        key = _setting_key("avatar_items", player_id)
        items = _get_json_setting(context, key, [])
        existing_descs = {str(item.get("AvatarItemDesc") if isinstance(item, dict) else item) for item in items}
        if item_desc not in existing_descs:
            items.append({"AvatarItemDesc": item_desc, "UnlockedLevel": unlocked_level})
            _set_json_setting(context, key, items)
    return Response(status_code=204)


def _gift_key(player_id: int) -> str:
    return _setting_key("avatar_gifts", player_id)


async def _handle_get_gifts(route_path: str, context) -> Response:
    match = re.fullmatch(r"api/avatar/v1/gifts/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player_id = int(match.group(1))
    _ensure_existing_profile(context, player_id)
    gifts = _get_json_setting(context, _gift_key(player_id), [])
    return JSONResponse(gifts)


async def _handle_create_gift(request: Request, route_path: str, context) -> Response:
    match = re.fullmatch(r"api/avatar/v1/gifts/create/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player_id = int(match.group(1))
    _ensure_existing_profile(context, player_id)
    payload = await _parse_client_payload(request)
    gifts = _get_json_setting(context, _gift_key(player_id), [])
    next_id = max([int(gift.get("Id") or 0) for gift in gifts if isinstance(gift, dict)] + [0]) + 1
    gift = {
        "Id": next_id,
        "AvatarItemDesc": _str_field(payload, "AvatarItemDesc", "avatarItemDesc"),
        "Xp": max(0, _int_field(payload, "Xp", "xp", default=0)),
    }
    gifts.append(gift)
    _set_json_setting(context, _gift_key(player_id), gifts)
    return JSONResponse(gift)


async def _handle_consume_gift(request: Request, context) -> Response:
    payload = await _parse_client_payload(request)
    player_id = _int_field(payload, "PlayerId", "playerId", "player_id", default=0)
    gift_id = _int_field(payload, "Id", "id", default=0)
    if player_id <= 0:
        raise HTTPException(status_code=400, detail="PlayerId is required.")
    _ensure_existing_profile(context, player_id)
    gifts = _get_json_setting(context, _gift_key(player_id), [])
    gifts = [gift for gift in gifts if not (isinstance(gift, dict) and int(gift.get("Id") or 0) == gift_id)]
    _set_json_setting(context, _gift_key(player_id), gifts)
    return Response(status_code=204)


def _player_settings_for_client(settings: dict[str, Any]) -> list[dict[str, str]]:
    return [{"Key": str(key), "Value": str(value)} for key, value in sorted(settings.items())]


async def _handle_get_settings(route_path: str, context) -> Response:
    match = re.fullmatch(r"api/settings/v1/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player_id = int(match.group(1))
    _ensure_existing_profile(context, player_id)
    settings = _get_json_setting(context, _setting_key("player_settings", player_id), {})
    return JSONResponse(_player_settings_for_client(settings))


async def _handle_set_setting(request: Request, context) -> Response:
    payload = await _parse_client_payload(request)
    player_id = _int_field(payload, "PlayerId", "playerId", "player_id", default=0)
    key_name = _str_field(payload, "Key", "key")
    if player_id <= 0:
        raise HTTPException(status_code=400, detail="PlayerId is required.")
    if not key_name:
        raise HTTPException(status_code=400, detail="Key is required.")
    _ensure_existing_profile(context, player_id)
    key = _setting_key("player_settings", player_id)
    settings = _get_json_setting(context, key, {})
    settings[key_name] = _str_field(payload, "Value", "value")
    _set_json_setting(context, key, settings)
    return Response(status_code=204)


async def _handle_remove_setting(request: Request, context) -> Response:
    payload = await _parse_client_payload(request)
    player_id = _int_field(payload, "PlayerId", "playerId", "player_id", default=0)
    key_name = _str_field(payload, "Key", "key")
    if player_id <= 0:
        raise HTTPException(status_code=400, detail="PlayerId is required.")
    if not key_name:
        raise HTTPException(status_code=400, detail="Key is required.")
    _ensure_existing_profile(context, player_id)
    key = _setting_key("player_settings", player_id)
    settings = _get_json_setting(context, key, {})
    settings.pop(key_name, None)
    _set_json_setting(context, key, settings)
    return Response(status_code=204)


def _request_app_version(request: Request) -> str:
    return str(request.headers.get("X-Rec-Room-Version") or request.headers.get("x-rec-room-version") or "").strip()


def _default_presence(player_id: int, app_version: str = "") -> dict[str, Any]:
    return {
        "PlayerId": player_id,
        "IsOnline": True,
        "GameSessionId": "",
        "AppVersion": app_version,
        "LastUpdateTime": _dotnet_utc_ticks(),
        "Activity": "",
        "Private": True,
        "AvailableSpace": 0,
        "GameInProgress": False,
    }


def _presence_for_player(context, player_id: int, app_version: str = "") -> dict[str, Any]:
    presence = _get_json_setting(context, _setting_key("presence", player_id), {})
    if not presence:
        return _default_presence(player_id, app_version)
    result = _default_presence(player_id, app_version)
    result.update({key: value for key, value in presence.items() if key in result})
    result["PlayerId"] = player_id
    if not result.get("AppVersion"):
        result["AppVersion"] = app_version
    result["LastUpdateTime"] = int(result.get("LastUpdateTime") or _dotnet_utc_ticks())
    return result


async def _handle_get_presence(request: Request, route_path: str, context) -> Response:
    match = re.fullmatch(r"api/presence/v1/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player_id = int(match.group(1))
    _ensure_existing_profile(context, player_id)
    return JSONResponse(_presence_for_player(context, player_id, _request_app_version(request)))


async def _handle_update_presence(request: Request, route_path: str, context) -> Response:
    match = re.fullmatch(r"api/presence/v1/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player_id = int(match.group(1))
    _ensure_existing_profile(context, player_id)
    payload = await _parse_client_payload(request)
    app_version = _str_field(payload, "AppVersion", "appVersion") or _request_app_version(request)
    presence = _default_presence(player_id, app_version)
    presence.update(
        {
            "PlayerId": player_id,
            "IsOnline": _bool_field(payload, "IsOnline", "isOnline", "Online", "online", default=True),
            "GameSessionId": _str_field(payload, "GameSessionId", "gameSessionId"),
            "AppVersion": app_version,
            "Activity": _str_field(payload, "Activity", "activity"),
            "Private": _bool_field(payload, "Private", "private", default=False),
            "AvailableSpace": _int_field(payload, "AvailableSpace", "availableSpace", default=0),
            "GameInProgress": _bool_field(payload, "GameInProgress", "gameInProgress", default=False),
            "LastUpdateTime": _dotnet_utc_ticks(),
        }
    )
    _set_json_setting(context, _setting_key("presence", player_id), presence)
    return Response(status_code=204)


async def _handle_presence_list(request: Request, context) -> Response:
    payload = await _parse_body_any(request)
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="Presence list payload must be a JSON list.")
    presences: list[dict[str, Any]] = []
    app_version = _request_app_version(request)
    for raw_id in payload:
        try:
            player_id = int(raw_id)
        except Exception:
            continue
        if _PLATFORM_BASE._find_player_by_legacy_id(context, player_id) is not None:
            presences.append(_presence_for_player(context, player_id, app_version))
    return JSONResponse(presences)


def _all_relationships(context) -> dict[str, dict[str, int]]:
    return _get_json_setting(context, _setting_key("relationships"), {})


def _save_relationships(context, relationships: dict[str, dict[str, int]]) -> None:
    _set_json_setting(context, _setting_key("relationships"), relationships)


def _relationship_for(relationships: dict[str, dict[str, int]], local_id: int, remote_id: int) -> int:
    return int(relationships.get(str(local_id), {}).get(str(remote_id), REL_NONE))


def _set_relationship(relationships: dict[str, dict[str, int]], local_id: int, remote_id: int, relationship_type: int) -> None:
    local = relationships.setdefault(str(local_id), {})
    if relationship_type == REL_NONE:
        local.pop(str(remote_id), None)
    else:
        local[str(remote_id)] = int(relationship_type)


def _relationship_response(relationships: dict[str, dict[str, int]], local_id: int, remote_id: int) -> dict[str, int]:
    return {"PlayerID": remote_id, "RelationshipType": _relationship_for(relationships, local_id, remote_id)}


async def _handle_get_relationships(route_path: str, context) -> Response:
    match = re.fullmatch(r"api/relationships/v1/get/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player_id = int(match.group(1))
    _ensure_existing_profile(context, player_id)
    relationships = _all_relationships(context)
    result = [
        {"PlayerID": int(remote_id), "RelationshipType": int(rel_type)}
        for remote_id, rel_type in sorted(relationships.get(str(player_id), {}).items(), key=lambda item: int(item[0]))
    ]
    return JSONResponse(result)


async def _handle_relationship_action(request: Request, action: str, context) -> Response:
    id1 = _int_field(dict(request.query_params), "id1", default=0)
    id2 = _int_field(dict(request.query_params), "id2", default=0)
    if id1 <= 0 or id2 <= 0:
        raise HTTPException(status_code=400, detail="id1 and id2 are required.")
    _ensure_existing_profile(context, id1)
    _ensure_existing_profile(context, id2)
    relationships = _all_relationships(context)
    if action in {"addfriend", "acceptfriendrequest"}:
        _set_relationship(relationships, id1, id2, REL_FRIEND)
        _set_relationship(relationships, id2, id1, REL_FRIEND)
    elif action == "removefriend":
        _set_relationship(relationships, id1, id2, REL_NONE)
        _set_relationship(relationships, id2, id1, REL_NONE)
    elif action == "sendfriendrequest":
        _set_relationship(relationships, id1, id2, REL_FRIEND_REQUEST_SENT)
        _set_relationship(relationships, id2, id1, REL_FRIEND_REQUEST_RECEIVED)
    elif action == "blockplayer":
        remote_rel = _relationship_for(relationships, id2, id1)
        _set_relationship(relationships, id1, id2, REL_BLOCKED_MUTUAL if remote_rel == REL_BLOCKED_LOCAL else REL_BLOCKED_LOCAL)
        _set_relationship(relationships, id2, id1, REL_BLOCKED_MUTUAL if remote_rel == REL_BLOCKED_LOCAL else REL_BLOCKED_REMOTE)
    elif action == "unblockplayer":
        _set_relationship(relationships, id1, id2, REL_NONE)
        if _relationship_for(relationships, id2, id1) in {REL_BLOCKED_REMOTE, REL_BLOCKED_MUTUAL}:
            _set_relationship(relationships, id2, id1, REL_NONE)
    else:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    _save_relationships(context, relationships)
    return JSONResponse(_relationship_response(relationships, id1, id2))


def _all_messages(context) -> list[dict[str, Any]]:
    return _get_json_setting(context, _setting_key("messages"), [])


def _save_messages(context, messages: list[dict[str, Any]]) -> None:
    _set_json_setting(context, _setting_key("messages"), messages)


async def _handle_get_messages(route_path: str, context) -> Response:
    match = re.fullmatch(r"api/messages/v1/get/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player_id = int(match.group(1))
    _ensure_existing_profile(context, player_id)
    messages = [message for message in _all_messages(context) if int(message.get("ToPlayerId") or 0) == player_id]
    return JSONResponse(
        [
            {
                "Id": int(message.get("Id") or 0),
                "FromPlayerId": int(message.get("FromPlayerId") or 0),
                "SentTime": int(message.get("SentTime") or _dotnet_utc_ticks()),
                "Type": int(message.get("Type") or 0),
                "Data": str(message.get("Data") or ""),
            }
            for message in messages
        ]
    )


async def _handle_send_message(request: Request, context) -> Response:
    payload = await _parse_client_payload(request)
    from_id = _int_field(payload, "FromPlayerId", "fromPlayerId", default=0)
    to_id = _int_field(payload, "ToPlayerId", "toPlayerId", default=0)
    if from_id <= 0 or to_id <= 0:
        raise HTTPException(status_code=400, detail="FromPlayerId and ToPlayerId are required.")
    _ensure_existing_profile(context, from_id)
    _ensure_existing_profile(context, to_id)
    messages = _all_messages(context)
    next_id = max([int(message.get("Id") or 0) for message in messages if isinstance(message, dict)] + [0]) + 1
    messages.append(
        {
            "Id": next_id,
            "FromPlayerId": from_id,
            "ToPlayerId": to_id,
            "SentTime": _dotnet_utc_ticks(),
            "Type": _int_field(payload, "Type", "type", default=0),
            "Data": _str_field(payload, "Data", "data"),
        }
    )
    _save_messages(context, messages)
    return Response(status_code=204)


async def _handle_delete_message(request: Request, context) -> Response:
    payload = await _parse_client_payload(request)
    message_id = _int_field(payload, "Id", "id", default=0)
    messages = [message for message in _all_messages(context) if int(message.get("Id") or 0) != message_id]
    _save_messages(context, messages)
    return Response(status_code=204)


async def _handle_game_sessions(route_path: str, context) -> Response:
    path = _clean_route_path(route_path)
    if path.casefold() in {"api/gamesessions/v1", "api/gamesessions/v1/"}:
        return JSONResponse([])
    match = re.fullmatch(r"api/gamesessions/v1/(.+)/?", path, flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    raise HTTPException(status_code=404, detail="Game session not found.")


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path in {"api/config/v1/motd", "api/config/v1/motd/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="MOTD method is not implemented.")
        return PlainTextResponse(context.get_motd(API_VERSION), media_type="text/plain; charset=utf-8")
    if path in {"api/config/v1/objectives", "api/config/v1/objectives/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Daily objectives method is not implemented.")
        return JSONResponse(DAILY_OBJECTIVES)

    if path.startswith("api/images/v1/profile/"):
        if method == "GET":
            return await _handle_get_profile_image(request, route_path, context)
        if method == "POST":
            return await _handle_set_profile_image(request, route_path, context)
        raise HTTPException(status_code=501, detail="Profile image method is not implemented.")

    if method == "GET" and path in {"api/players/v1", "api/players/v1/"}:
        return await _handle_get_profile_by_platform(request, context)
    if method == "GET" and re.fullmatch(r"api/players/v1/\d+/?", path):
        return await _handle_get_profile_by_id(route_path, context)
    if path in {"api/players/v1/create", "api/players/v1/create/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player create method is not implemented.")
        return await _handle_create_profile(request, route_path, context)
    if path in {"api/players/v1/list", "api/players/v1/list/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player list method is not implemented.")
        return await _handle_profile_list(request, context)
    if path.startswith("api/players/v1/updatereputation/"):
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player reputation method is not implemented.")
        return await _handle_update_reputation(request, route_path, context)
    if path.startswith("api/players/v1/objective/"):
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player objective method is not implemented.")
        return await _handle_complete_objective(request, route_path, context)

    if re.fullmatch(r"api/avatar/v1/gifts/\d+/?", path):
        if method != "GET":
            raise HTTPException(status_code=501, detail="Avatar gifts method is not implemented.")
        return await _handle_get_gifts(route_path, context)
    if path.startswith("api/avatar/v1/gifts/create/"):
        if method != "POST":
            raise HTTPException(status_code=501, detail="Avatar gift create method is not implemented.")
        return await _handle_create_gift(request, route_path, context)
    if path in {"api/avatar/v1/gifts/consume", "api/avatar/v1/gifts/consume/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Avatar gift consume method is not implemented.")
        return await _handle_consume_gift(request, context)
    if re.fullmatch(r"api/avatar/v2/items/\d+/?", path):
        if method != "GET":
            raise HTTPException(status_code=501, detail="Avatar items method is not implemented.")
        return await _handle_get_avatar_items(route_path, context)
    if path in {"api/avatar/v1/items/create", "api/avatar/v1/items/create/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Avatar item create method is not implemented.")
        return await _handle_create_avatar_item(request, context)
    if re.fullmatch(r"api/avatar/v1/\d+/?", path):
        if method != "GET":
            raise HTTPException(status_code=501, detail="Avatar method is not implemented.")
        return await _handle_get_avatar(route_path, context)
    if path in {"api/avatar/v1/set", "api/avatar/v1/set/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Avatar set method is not implemented.")
        return await _handle_set_avatar(request, context)

    if re.fullmatch(r"api/settings/v1/\d+/?", path):
        if method != "GET":
            raise HTTPException(status_code=501, detail="Settings method is not implemented.")
        return await _handle_get_settings(route_path, context)
    if path in {"api/settings/v1/set", "api/settings/v1/set/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Settings set method is not implemented.")
        return await _handle_set_setting(request, context)
    if path in {"api/settings/v1/remove", "api/settings/v1/remove/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Settings remove method is not implemented.")
        return await _handle_remove_setting(request, context)

    if path in {"api/presence/v1/list", "api/presence/v1/list/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Presence list method is not implemented.")
        return await _handle_presence_list(request, context)
    if re.fullmatch(r"api/presence/v1/\d+/?", path):
        if method == "GET":
            return await _handle_get_presence(request, route_path, context)
        if method == "POST":
            return await _handle_update_presence(request, route_path, context)
        raise HTTPException(status_code=501, detail="Presence method is not implemented.")

    if re.fullmatch(r"api/relationships/v1/get/\d+/?", path):
        if method != "GET":
            raise HTTPException(status_code=501, detail="Relationships method is not implemented.")
        return await _handle_get_relationships(route_path, context)
    for action in ("addfriend", "removefriend", "sendfriendrequest", "acceptfriendrequest", "blockplayer", "unblockplayer"):
        if path in {f"api/relationships/v1/{action}", f"api/relationships/v1/{action}/"}:
            if method != "GET":
                raise HTTPException(status_code=501, detail="Relationship action method is not implemented.")
            return await _handle_relationship_action(request, action, context)

    if re.fullmatch(r"api/messages/v1/get/\d+/?", path):
        if method != "GET":
            raise HTTPException(status_code=501, detail="Messages method is not implemented.")
        return await _handle_get_messages(route_path, context)
    if path in {"api/messages/v1/send", "api/messages/v1/send/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Message send method is not implemented.")
        return await _handle_send_message(request, context)
    if path in {"api/messages/v1/delete", "api/messages/v1/delete/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Message delete method is not implemented.")
        return await _handle_delete_message(request, context)

    if path in {"api/gamesessions/v1", "api/gamesessions/v1/"} or path.startswith("api/gamesessions/v1/"):
        if method != "GET":
            raise HTTPException(status_code=501, detail="Game session method is not implemented.")
        return await _handle_game_sessions(route_path, context)

    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    path = _clean_route_path(route_path).casefold()
    if path not in {"api/notification/v1", "api/notification/v1/"}:
        await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
        return
    await websocket.accept()
    try:
        await websocket.receive_text()
        await websocket.send_text("OK")
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        return
