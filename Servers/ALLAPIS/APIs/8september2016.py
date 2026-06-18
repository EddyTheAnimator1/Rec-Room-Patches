"""8 September 2016 Rec Room HTTP API adapter.

Confirmed from decompiled client build 394935228226334493:
- GET  api/players/v1/?p=<platform>&id=<platform id>
- POST api/players/v1/create with form fields Platform, PlatformId, and Name
- POST api/players/v1/update/<Id> with WebManager.PlayerModel JSON
- GET  api/images/v1/profile/<Id>
- POST api/images/v1/profile/<Id> with multipart form field image
- GET  motd, for patched MOTD URL convenience
- GET  api/tournament?player=<Photon player name>
- GET  api/tournament/forfeit?match=<MatchId>&player=<Photon player name>

The player and tournament surface is the same as 31august2016. This build adds
profile image get/post calls under api/images/v1/profile/<Id>.
"""

from __future__ import annotations

import importlib.util
import mimetypes
import re
from email.parser import BytesParser
from email.policy import default as email_default_policy
from pathlib import Path

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import FileResponse, Response

API_VERSION = "8september2016"
PROFILE_IMAGE_PURPOSE = "shared.profile_image"
DEFAULT_PROFILE_IMAGE_BYTES = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4"
    b"\x89\x00\x00\x00\rIDATx\x9cc\xf8\xff\xff?\x00\x05"
    b"\xfe\x02\xfeA\xe2 \x9a\x00\x00\x00\x00IEND\xaeB`\x82"
)


def _load_base_adapter():
    module_path = Path(__file__).with_name("31august2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_31august2016_shared_for_8september2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 31august2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    return module


_BASE = _load_base_adapter()


def _route(route_path: str) -> str:
    return route_path.strip("/").casefold()


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


async def _handle_get_profile_image(route_path: str, context) -> Response:
    legacy_id = _legacy_id_from_image_route(route_path)
    if legacy_id is None:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player = _BASE._find_player_by_legacy_id(context, legacy_id)
    if player is None:
        return Response(content=DEFAULT_PROFILE_IMAGE_BYTES, media_type="image/png")
    context.assert_player_not_banned(player["player_id"])
    asset = _image_asset_for_player(context, player["player_id"])
    if asset is None:
        return Response(content=DEFAULT_PROFILE_IMAGE_BYTES, media_type="image/png")
    image_path = (context.data_dir / asset["relative_path"]).resolve()
    data_dir = context.data_dir.resolve()
    if data_dir not in image_path.parents or not image_path.is_file():
        return Response(content=DEFAULT_PROFILE_IMAGE_BYTES, media_type="image/png")
    return FileResponse(image_path, media_type=asset["mime_type"])


async def _handle_set_profile_image(request: Request, route_path: str, context) -> Response:
    legacy_id = _legacy_id_from_image_route(route_path)
    if legacy_id is None:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    player = _BASE._find_player_by_legacy_id(context, legacy_id)
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


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _route(route_path)
    if path.startswith("api/images/v1/profile/"):
        if request.method == "GET":
            return await _handle_get_profile_image(route_path, context)
        if request.method == "POST":
            return await _handle_set_profile_image(request, route_path, context)
        raise HTTPException(status_code=501, detail="Profile image method is not implemented.")
    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
