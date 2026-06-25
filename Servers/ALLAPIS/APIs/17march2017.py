"""17 March 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from the game build at manifest 1867696127010072960:
- RecNet still uses COGCNMJCNKN.
- HTTP/WebSocket URL fields remain EHBCBOGDLDB and FPGKGDJLOJJ.
- Login posts the same real /api/platformlogin/v1 form as 13 March.
- Player subscription synchronization remains notification-WebSocket driven;
  REST api/PlayerSubscriptions/v1/init/add/remove is not a real route here.
- Messages include v2 get/send/delete, v1 sendMultiple, and offline invites.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import Response

API_VERSION = "17march2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Fri, 17 Mar 2017 19:39:02 GMT"


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
    module_path = Path(__file__).with_name("13march2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_13march2017_shared_for_17march2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 13march2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()
_BASE = _SHARED._BASE
_PLATFORM_BASE = _SHARED._PLATFORM_BASE


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _player_from_row(row) -> dict[str, Any]:
    player = {key: row[key] for key in row.keys() if key != "state_json"}
    try:
        state = json.loads(row["state_json"] or "{}")
    except Exception:
        state = {}
    player["state"] = state if isinstance(state, dict) else {}
    return player


def _find_player_by_recnet_id(context, recnet_id: int) -> dict[str, Any] | None:
    with context.db.connection() as conn:
        row = conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players AS p
            JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND CAST(json_extract(pvs.state_json, '$.recnet_id') AS INTEGER) = ?
            LIMIT 1
            """,
            (API_VERSION, recnet_id),
        ).fetchone()
        if row is None:
            row = conn.execute(
                """
                SELECT p.*, pvs.state_json
                FROM players AS p
                JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
                WHERE CAST(json_extract(pvs.state_json, '$.recnet_id') AS INTEGER) = ?
                ORDER BY pvs.updated_at DESC
                LIMIT 1
                """,
                (recnet_id,),
            ).fetchone()
    return _player_from_row(row) if row is not None else None


def _find_player_by_profile_image_id(context, profile_id: int) -> dict[str, Any] | None:
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, profile_id)
    if player is not None:
        return player
    return _find_player_by_recnet_id(context, profile_id)


def _profile_image_response(request: Request, *, content: bytes, media_type: str, last_modified: str) -> Response:
    headers = {
        "Last-Modified": last_modified,
        "other_player_id": last_modified,
        "reported_player_rep": last_modified,
    }
    if _BASE._same_http_date(str(request.headers.get("if-modified-since") or ""), last_modified):
        return Response(status_code=304, headers=headers)
    for name in ("amount", "reported_player_rep"):
        if _BASE._same_http_date(str(request.headers.get(name) or ""), last_modified):
            return Response(status_code=189, headers=headers)
    return Response(content=content, media_type=media_type, headers=headers)


async def _handle_get_profile_image(request: Request, route_path: str, context) -> Response:
    match = re.fullmatch(r"api/images/v1/profile/(\d+)/?", _clean_route_path(route_path), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player = _find_player_by_profile_image_id(context, int(match.group(1)))
    if player is None:
        return _profile_image_response(
            request,
            content=_BASE.DEFAULT_PROFILE_IMAGE_BYTES,
            media_type="image/png",
            last_modified=_BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED,
        )
    context.assert_player_not_banned(player["player_id"])
    asset = _BASE._image_asset_for_player(context, player["player_id"])
    if asset is None:
        return _profile_image_response(
            request,
            content=_BASE.DEFAULT_PROFILE_IMAGE_BYTES,
            media_type="image/png",
            last_modified=_BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED,
        )
    image_path = (context.data_dir / asset["relative_path"]).resolve()
    data_dir = context.data_dir.resolve()
    if data_dir not in image_path.parents or not image_path.is_file():
        return _profile_image_response(
            request,
            content=_BASE.DEFAULT_PROFILE_IMAGE_BYTES,
            media_type="image/png",
            last_modified=_BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED,
        )
    return _profile_image_response(
        request,
        content=image_path.read_bytes(),
        media_type=str(asset["mime_type"] or "application/octet-stream"),
        last_modified=_BASE._http_date_from_created_at(asset["created_at"]),
    )


async def _handle_profile_image_v2(request: Request, context) -> Response:
    player_id = _SHARED._ensure_local_profile(request, context)
    player = _find_player_by_profile_image_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])

    body = await request.body()
    content, declared_mime_type = _BASE._parse_multipart_image(str(request.headers.get("content-type") or ""), body)
    if not content:
        raise HTTPException(status_code=400, detail="image form field is empty.")
    mime_type, file_ext = _BASE._detect_image_type(content, declared_mime_type)
    try:
        asset = context.save_image_bytes(
            owner_player_id=player["player_id"],
            content=content,
            file_ext=file_ext,
            mime_type=mime_type,
            purpose=_BASE.PROFILE_IMAGE_PURPOSE,
            metadata={"legacy_player_id": player_id, "api_version": API_VERSION},
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
              AND is_coach = 0
            """,
            (asset["asset_id"], player["player_id"]),
        )
    return Response(status_code=204)


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path.startswith("api/images/v1/profile/"):
        if method != "GET":
            raise HTTPException(status_code=501, detail="Profile image method is not implemented.")
        return await _handle_get_profile_image(request, route_path, context)

    if path in {"api/images/v2/profile", "api/images/v2/profile/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Profile image upload method is not implemented.")
        return await _handle_profile_image_v2(request, context)

    return await _SHARED.handle_http(request=request, route_path=route_path, context=context)

handle_websocket = _SHARED.handle_websocket
