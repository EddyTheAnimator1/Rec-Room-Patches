"""RecNet HTTP surface for the 23 December 2016 build.

Confirmed from first-party non-Photon client code in Assembly-CSharp.dll.
"""

from __future__ import annotations

import base64
import json
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs

from fastapi import HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response


API_VERSION = "23december2016"

_TRANSPARENT_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMB/6X"
    "n2Z8AAAAASUVORK5CYII="
)

_RELATIONSHIP_TYPES = {
    "addfriend": 3,
    "removefriend": 0,
    "sendfriendrequest": 1,
    "acceptfriendrequest": 3,
    "blockplayer": 4,
    "unblockplayer": 0,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _now_ticks() -> int:
    # .NET ticks are 100ns intervals since 0001-01-01.
    return int(time.time() * 10_000_000) + 621355968000000000


def _safe_username(value: str | None, fallback: str) -> str:
    value = (value or "").strip()
    if not value:
        value = fallback
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^A-Za-z0-9_.-]", "", value)
    return value[:32] or fallback


async def _body_bytes(request: Request) -> bytes:
    try:
        return await request.body()
    except Exception:
        return b""


async def _json_body(request: Request, default: Any) -> Any:
    body = await _body_bytes(request)
    if not body:
        return default
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        return default


async def _form_body(request: Request) -> dict[str, str]:
    body = await _body_bytes(request)
    parsed = parse_qs(body.decode("utf-8", errors="ignore"), keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def _int_value(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _image_type(content: bytes) -> tuple[str, str] | None:
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png", "image/png"
    if content.startswith(b"\xff\xd8\xff"):
        return ".jpg", "image/jpeg"
    return None


def _multipart_field(body: bytes, content_type: str, field_name: str) -> bytes | None:
    match = re.search(r'boundary="?([^";]+)"?', content_type)
    if not match:
        return None
    boundary = b"--" + match.group(1).encode("utf-8")
    for raw_part in body.split(boundary):
        part = raw_part.strip()
        if not part or part == b"--":
            continue
        if part.endswith(b"--"):
            part = part[:-2].rstrip()
        header_blob, separator, payload = part.partition(b"\r\n\r\n")
        if not separator:
            continue
        headers = header_blob.decode("utf-8", errors="ignore")
        disposition_match = re.search(
            r'content-disposition:.*name="' + re.escape(field_name) + r'"',
            headers,
            re.IGNORECASE,
        )
        if disposition_match:
            return payload.rstrip(b"\r\n")
    return None


def _profile_header(request: Request) -> int | None:
    value = request.headers.get("x-rec-room-profile")
    if not value:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _load_state(context: Any, player_id: str) -> dict[str, Any]:
    with context.db.connect() as conn:
        row = conn.execute(
            "SELECT state_json FROM player_version_state WHERE player_id = ? AND api_version = ?",
            (player_id, API_VERSION),
        ).fetchone()
    if not row:
        return {}
    try:
        return json.loads(row["state_json"])
    except Exception:
        return {}


def _save_state(context: Any, player_id: str, state: dict[str, Any]) -> None:
    stamp = _now_iso()
    with context.db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO player_version_state (player_id, api_version, state_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(player_id, api_version) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (player_id, API_VERSION, json.dumps(state, sort_keys=True), stamp, stamp),
        )


def _row_by_recnet_id(context: Any, recnet_id: int) -> Any | None:
    with context.db.connect() as conn:
        return conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players p
            JOIN player_version_state pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND json_extract(pvs.state_json, '$.recnet_id') = ?
            """,
            (API_VERSION, recnet_id),
        ).fetchone()


def _row_by_platform(context: Any, platform: int, platform_id: str) -> Any | None:
    with context.db.connect() as conn:
        return conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players p
            JOIN player_version_state pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND json_extract(pvs.state_json, '$.platform') = ?
              AND json_extract(pvs.state_json, '$.platform_id') = ?
            """,
            (API_VERSION, platform, platform_id),
        ).fetchone()


def _allocated_recnet_id(context: Any, player_id: str, preferred: int) -> int:
    if preferred > 0:
        existing = _row_by_recnet_id(context, preferred)
        if not existing or existing["player_id"] == player_id:
            return preferred
    seed = abs(hash(player_id)) % 8_000_000_000
    candidate = 1_000_000_000 + seed
    while True:
        existing = _row_by_recnet_id(context, candidate)
        if not existing or existing["player_id"] == player_id:
            return candidate
        candidate += 1


def _ensure_player(
    context: Any,
    platform: int,
    platform_id: str,
    name: str | None,
) -> dict[str, Any]:
    identity_key = f"platform:{platform}:{platform_id}"
    username = _safe_username(name, f"Player{platform_id or 'Local'}")
    player = context.get_or_create_player(
        API_VERSION,
        identity_key=identity_key,
        username=username,
        display_name=(name or username),
    )
    state = _load_state(context, player["player_id"])
    recnet_id = state.get("recnet_id")
    if not recnet_id:
        recnet_id = _allocated_recnet_id(context, player["player_id"], _int_value(platform_id))
    state.setdefault("identity_key", identity_key)
    state["recnet_id"] = int(recnet_id)
    state["platform"] = platform
    state["platform_id"] = str(platform_id)
    state.setdefault("reputation", 0)
    state.setdefault("avatar", {"OutfitSelections": "", "SkinColor": "", "HairColor": ""})
    _seed_new_player_preferences(state)
    _save_state(context, player["player_id"], state)
    return _profile_from_player(player, state)


def _seed_new_player_preferences(state: dict[str, Any]) -> bool:
    settings = state.get("settings")
    if not isinstance(settings, dict):
        settings = {}
        state["settings"] = settings
    if state.get("new_player_preferences_seeded"):
        return False
    settings["Recroom.OOBE"] = "0"
    settings.pop("OBJECTIVE_DATE", None)
    for index in range(3):
        settings.pop(f"OBJECTIVE_PROGRESS{index}", None)
        settings.pop(f"OBJECTIVE_COMPLETED{index}", None)
    state["new_player_preferences_seeded"] = True
    return True


def _profile_from_row(row: Any) -> dict[str, Any]:
    try:
        state = json.loads(row["state_json"])
    except Exception:
        state = {}
    return _profile_from_player(row, state)


def _profile_from_player(player: Any, state: dict[str, Any]) -> dict[str, Any]:
    return {
        "Id": int(state.get("recnet_id") or 0),
        "Username": player["username"],
        "DisplayName": player["display_name"],
        "XP": int(player["canonical_xp"] or 0),
        "Level": int(player["canonical_level"] or 1),
        "Reputation": int(state.get("reputation") or 0),
        "Verified": bool(player["verified"]),
    }


def _current_player(context: Any, request: Request) -> tuple[Any | None, dict[str, Any]]:
    recnet_id = _profile_header(request)
    if recnet_id is None:
        return None, {}
    row = _row_by_recnet_id(context, recnet_id)
    if not row:
        return None, {}
    try:
        state = json.loads(row["state_json"])
    except Exception:
        state = {}
    return row, state


def _profile_image_response(context: Any, recnet_id: int) -> Response:
    row = _row_by_recnet_id(context, recnet_id)
    if row and row["profile_picture_asset_id"]:
        with context.db.connect() as conn:
            asset = conn.execute(
                """
                SELECT relative_path, mime_type, created_at
                FROM data_assets
                WHERE asset_id = ?
                """,
                (row["profile_picture_asset_id"],),
            ).fetchone()
        if asset:
            image_path = context.data_dir / asset["relative_path"]
            if image_path.is_file():
                return Response(
                    image_path.read_bytes(),
                    media_type=asset["mime_type"],
                    headers={"Last-Modified": "Fri, 23 Dec 2016 02:09:34 GMT"},
                )
    return Response(
        _TRANSPARENT_PNG,
        media_type="image/png",
        headers={"Last-Modified": "Fri, 23 Dec 2016 02:09:33 GMT"},
    )


async def _store_profile_image(request: Request, context: Any) -> Response:
    row, _ = _current_player(context, request)
    if not row:
        raise HTTPException(status_code=404, detail="Player not found")

    body = await _body_bytes(request)
    content_type = request.headers.get("content-type", "")
    image = _multipart_field(body, content_type, "image")
    if image is None and _image_type(body):
        image = body
    if not image:
        raise HTTPException(status_code=400, detail="Missing profile image")

    detected = _image_type(image)
    if detected is None:
        raise HTTPException(status_code=400, detail="Unsupported profile image format")
    file_ext, mime_type = detected
    asset = context.save_image_bytes(
        owner_player_id=row["player_id"],
        content=image,
        file_ext=file_ext,
        mime_type=mime_type,
        purpose=f"{API_VERSION}.profile_image",
        metadata={"recnet_id": _profile_header(request)},
    )
    with context.db.transaction() as conn:
        conn.execute(
            "UPDATE players SET profile_picture_asset_id = ?, updated_at = ? WHERE player_id = ?",
            (asset["asset_id"], _now_iso(), row["player_id"]),
        )
    return Response(status_code=200)


def _config_payload(context: Any) -> dict[str, Any]:
    return {
        "MessageOfTheDay": context.get_motd(API_VERSION),
        "MatchmakingParams": {
            "PreferFullRoomsFrequency": 0.7,
            "PreferEmptyRoomsFrequency": 0.3,
        },
        "DailyObjectives": [
            [
                {"type": 100, "score": 1},
                {"type": 201, "score": 1},
                {"type": 300, "score": 1},
            ],
            [
                {"type": 300, "score": 1},
                {"type": 401, "score": 1},
                {"type": 500, "score": 1},
            ],
            [
                {"type": 500, "score": 1},
                {"type": 802, "score": 1},
                {"type": 200, "score": 1},
            ],
            [
                {"type": 200, "score": 1},
                {"type": 302, "score": 1},
                {"type": 400, "score": 1},
            ],
            [
                {"type": 400, "score": 1},
                {"type": 603, "score": 1},
                {"type": 700, "score": 1},
            ],
            [
                {"type": 700, "score": 1},
                {"type": 101, "score": 1},
                {"type": 800, "score": 1},
            ],
            [
                {"type": 800, "score": 1},
                {"type": 502, "score": 1},
                {"type": 100, "score": 1},
            ],
        ],
        "ConfigTable": [
            {"Key": "Gift.DropChance", "Value": "0"},
            {"Key": "Gift.XP", "Value": "0"},
        ],
    }


def _avatar_for(row: Any | None, state: dict[str, Any]) -> dict[str, str]:
    avatar = state.get("avatar") if state else None
    if not isinstance(avatar, dict):
        avatar = {}
    return {
        "OutfitSelections": str(avatar.get("OutfitSelections") or ""),
        "SkinColor": str(avatar.get("SkinColor") or ""),
        "HairColor": str(avatar.get("HairColor") or ""),
    }


def _gift_rows(context: Any, player_id: str) -> list[dict[str, Any]]:
    with context.db.connect() as conn:
        rows = conn.execute(
            """
            SELECT gift_box_id, state_json
            FROM gift_boxes
            WHERE player_id = ?
            ORDER BY created_at ASC
            """,
            (player_id,),
        ).fetchall()
    gifts: list[dict[str, Any]] = []
    for row in rows:
        try:
            state = json.loads(row["state_json"])
        except Exception:
            state = {}
        if state.get("api_version") != API_VERSION or state.get("consumed"):
            continue
        gifts.append(
            {
                "Id": int(state.get("december_id") or 0),
                "AvatarItemDesc": "",
                "Xp": max(100, int(state.get("Xp") or 0)),
            }
        )
    return gifts


def _next_gift_id(context: Any) -> int:
    with context.db.connect() as conn:
        rows = conn.execute("SELECT state_json FROM gift_boxes").fetchall()
    current = 0
    for row in rows:
        try:
            state = json.loads(row["state_json"])
        except Exception:
            continue
        if state.get("api_version") == API_VERSION:
            current = max(current, int(state.get("december_id") or 0))
    return current + 1


async def _handle_players(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/players/v1/getorcreate" and method == "POST":
        form = await _form_body(request)
        profile = _ensure_player(
            context,
            _int_value(form.get("Platform")),
            str(form.get("PlatformId") or ""),
            form.get("Name"),
        )
        return JSONResponse(profile)

    if path == "api/players/v1/list" and method == "POST":
        ids = await _json_body(request, [])
        if not isinstance(ids, list):
            ids = []
        profiles = []
        for value in ids:
            row = _row_by_recnet_id(context, _int_value(value, -1))
            if row:
                profiles.append(_profile_from_row(row))
        return JSONResponse(profiles)

    match = re.fullmatch(r"api/players/v1/(\d+)", path)
    if match and method == "GET":
        row = _row_by_recnet_id(context, int(match.group(1)))
        if not row:
            raise HTTPException(status_code=404, detail="Player not found")
        return JSONResponse(_profile_from_row(row))

    if path in {"api/players/v2", "api/players/v2/"} and method == "GET":
        platform = _int_value(request.query_params.get("p"), -1)
        platform_id = str(request.query_params.get("id") or "")
        row = _row_by_platform(context, platform, platform_id)
        if not row:
            raise HTTPException(status_code=404, detail="Player not found")
        return JSONResponse(_profile_from_row(row))

    if path == "api/players/v2/verify" and method == "POST":
        return JSONResponse({"Message": "Verification email sent."})

    if path == "api/players/v2/updatereputation" and method == "POST":
        row, state = _current_player(context, request)
        form = await _form_body(request)
        if row:
            state["reputation"] = int(state.get("reputation") or 0) - _int_value(
                form.get("reputationDelta")
            )
            _save_state(context, row["player_id"], state)
        return Response(status_code=200)

    if path == "api/players/v2/objective" and method == "POST":
        row, state = _current_player(context, request)
        form = await _form_body(request)
        additional_xp = max(0, _int_value(form.get("additionalXp")))
        delta_xp = additional_xp if additional_xp else 10
        current_xp = delta_xp
        current_level = 1
        if row:
            total_xp = int(row["canonical_xp"] or 0) + delta_xp
            current_level = max(1, 1 + (total_xp // 1000))
            current_xp = total_xp % 1000
            with context.db.transaction() as conn:
                conn.execute(
                    "UPDATE players SET canonical_xp = ?, canonical_level = ?, updated_at = ? WHERE player_id = ?",
                    (total_xp, current_level, _now_iso(), row["player_id"]),
                )
        return JSONResponse(
            {
                "deltaXp": delta_xp,
                "currentLevel": current_level,
                "currentXp": current_xp,
                "xpRequiredToLevelUp": 1000,
            }
        )

    if path == "api/players/v1/score" and method == "POST":
        return Response(status_code=200)

    raise HTTPException(status_code=404, detail="Unknown player route")


async def _handle_avatar(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()
    row, state = _current_player(context, request)

    if path == "api/avatar/v2" and method == "GET":
        return JSONResponse(_avatar_for(row, state))

    if path == "api/avatar/v2/set" and method == "POST":
        avatar = await _json_body(request, {})
        if row and isinstance(avatar, dict):
            state["avatar"] = {
                "OutfitSelections": str(avatar.get("OutfitSelections") or ""),
                "SkinColor": str(avatar.get("SkinColor") or ""),
                "HairColor": str(avatar.get("HairColor") or ""),
            }
            _save_state(context, row["player_id"], state)
        return Response(status_code=200)

    if path == "api/avatar/v3/items" and method == "GET":
        return JSONResponse([])

    if path == "api/avatar/v2/gifts" and method == "GET":
        if not row:
            return JSONResponse([])
        return JSONResponse(_gift_rows(context, row["player_id"]))

    if path == "api/avatar/v2/gifts/create" and method == "POST":
        if not row:
            raise HTTPException(status_code=404, detail="Player not found")
        form = await _form_body(request)
        gift_id = _next_gift_id(context)
        stamp = _now_iso()
        state_json = {
            "api_version": API_VERSION,
            "december_id": gift_id,
            "AvatarItemDesc": "",
            "OriginalAvatarItemDesc": str(form.get("AvatarItemDesc") or ""),
            "Xp": max(100, _int_value(form.get("Xp"))),
            "consumed": False,
        }
        with context.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO gift_boxes (
                    gift_box_id, player_id, state_json, opened, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    f"{API_VERSION}:{gift_id}",
                    row["player_id"],
                    json.dumps(state_json),
                    0,
                    stamp,
                    stamp,
                ),
            )
        return JSONResponse(
            {
                "Id": gift_id,
                "AvatarItemDesc": state_json["AvatarItemDesc"],
                "Xp": state_json["Xp"],
            }
        )

    if path == "api/avatar/v2/gifts/consume" and method == "POST":
        form = await _form_body(request)
        gift_id = _int_value(form.get("Id"), -1)
        with context.db.transaction() as conn:
            rows = conn.execute(
                "SELECT gift_box_id, state_json FROM gift_boxes WHERE gift_box_id = ?",
                (f"{API_VERSION}:{gift_id}",),
            ).fetchall()
            for gift in rows:
                try:
                    gift_state = json.loads(gift["state_json"])
                except Exception:
                    gift_state = {}
                gift_state["consumed"] = True
                gift_state["UnlockedLevel"] = _int_value(form.get("UnlockedLevel"))
                conn.execute(
                    "UPDATE gift_boxes SET state_json = ?, opened = ?, updated_at = ? WHERE gift_box_id = ?",
                    (json.dumps(gift_state), 1, _now_iso(), gift["gift_box_id"]),
                )
        return Response(status_code=200)

    raise HTTPException(status_code=404, detail="Unknown avatar route")


async def _handle_settings(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()
    row, state = _current_player(context, request)

    if path in {"api/settings/v2", "api/settings/v2/"} and method == "GET":
        if row and _seed_new_player_preferences(state):
            _save_state(context, row["player_id"], state)
        settings = state.get("settings") if isinstance(state.get("settings"), dict) else {}
        return JSONResponse(
            [{"Key": str(key), "Value": str(value)} for key, value in settings.items()]
        )

    if path == "api/settings/v2/set" and method == "POST":
        payload = await _json_body(request, {})
        if row and isinstance(payload, dict):
            settings = state.get("settings") if isinstance(state.get("settings"), dict) else {}
            settings[str(payload.get("Key") or "")] = str(payload.get("Value") or "")
            state["settings"] = settings
            _save_state(context, row["player_id"], state)
        return Response(status_code=200)

    if path == "api/settings/v2/remove" and method == "POST":
        payload = await _json_body(request, {})
        if row and isinstance(payload, dict):
            settings = state.get("settings") if isinstance(state.get("settings"), dict) else {}
            settings.pop(str(payload.get("Key") or ""), None)
            state["settings"] = settings
            _save_state(context, row["player_id"], state)
        return Response(status_code=200)

    raise HTTPException(status_code=404, detail="Unknown settings route")


async def _handle_social(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/messages/v2/get" and method == "GET":
        return JSONResponse([])

    if path in {"api/messages/v2/send", "api/messages/v2/delete"} and method == "POST":
        return Response(status_code=200)

    if path == "api/relationships/v2/get" and method == "GET":
        return JSONResponse([])

    match = re.fullmatch(r"api/relationships/v2/([a-z]+)", path)
    if match and method == "GET" and match.group(1) in _RELATIONSHIP_TYPES:
        return JSONResponse(
            {
                "PlayerID": _int_value(request.query_params.get("id")),
                "RelationshipType": _RELATIONSHIP_TYPES[match.group(1)],
            }
        )

    raise HTTPException(status_code=404, detail="Unknown social route")


async def _handle_presence(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path == "api/presence/v1/list" and method == "POST":
        return JSONResponse([])

    if path == "api/presence/v2" and method == "POST":
        return Response(status_code=200)

    match = re.fullmatch(r"api/presence/v1/(\d+)", path)
    if match and method == "GET":
        raise HTTPException(status_code=404, detail="Presence not found")

    raise HTTPException(status_code=404, detail="Unknown presence route")


async def _handle_gamesessions(path: str, request: Request, context: Any) -> Response:
    method = request.method.upper()

    if path in {"api/gamesessions/v1", "api/gamesessions/v1/"} and method == "GET":
        return JSONResponse([])

    match = re.fullmatch(r"api/gamesessions/v1/([^/]+)", path)
    if match and method == "GET":
        raise HTTPException(status_code=404, detail="Game session not found")

    raise HTTPException(status_code=404, detail="Unknown game session route")


async def handle_http(route_path: str, request: Request, context: Any) -> Response:
    path = route_path.strip("/")
    method = request.method.upper()

    if path == "api/versioncheck/v1" and method == "GET":
        return Response(status_code=200)

    if path == "api/config/v2" and method == "GET":
        return JSONResponse(_config_payload(context))

    if path.startswith("api/players/"):
        return await _handle_players(path, request, context)

    if path.startswith("api/avatar/"):
        return await _handle_avatar(path, request, context)

    if path.startswith("api/settings/"):
        return await _handle_settings(path, request, context)

    if path.startswith("api/messages/") or path.startswith("api/relationships/"):
        return await _handle_social(path, request, context)

    if path.startswith("api/presence/"):
        return await _handle_presence(path, request, context)

    if path.startswith("api/gamesessions/"):
        return await _handle_gamesessions(path, request, context)

    match = re.fullmatch(r"api/images/v1/profile/(\d+)", path)
    if match and method == "GET":
        return _profile_image_response(context, int(match.group(1)))

    if path == "api/images/v2/profile" and method == "POST":
        return await _store_profile_image(request, context)

    if path == "api/analytics/v1/session/event" and method == "POST":
        return Response(status_code=200)

    if path in {"api/tournament", "api/tournament/forfeit"}:
        raise HTTPException(status_code=501, detail="Tournament API confirmed but not implemented")

    raise HTTPException(status_code=404, detail="Unknown route")


async def handle_websocket(route_path: str, websocket: WebSocket, context: Any) -> None:
    path = route_path.strip("/")
    if path != "api/notification/v2":
        await websocket.close(code=1008)
        return

    await websocket.accept()
    try:
        await websocket.receive_text()
        await websocket.send_text(json.dumps({"SessionId": _now_ticks()}))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        return
