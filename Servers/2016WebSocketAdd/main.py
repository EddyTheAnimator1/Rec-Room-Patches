from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, request

import rr23_shared as shared

app = Flask(__name__)

DATA_DIR = shared.DATA_DIR
IMAGES_DIR = DATA_DIR / "player_images"
MAX_REQUEST_BODY_BYTES = max(1024, int(os.environ.get("MAX_REQUEST_BODY_BYTES", str(4 * 1024 * 1024))))
ENABLE_DEBUG_ENDPOINTS = os.environ.get("ENABLE_DEBUG_ENDPOINTS", "false").strip().lower() in {"1", "true", "yes", "y"}
REQUIRE_HTTP_AUTH = os.environ.get("REQUIRE_HTTP_AUTH", os.environ.get("REQUIRE_AUTH", "false")).strip().lower() in {"1", "true", "yes", "y"}

TRANSPARENT_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9s2w0v8AAAAASUVORK5CYII="
)
DEFAULT_OBJECTIVES = [
    {
        "Date": "fallback",
        "Objectives": [
            {"ObjectiveType": 301, "RequiredScore": 1, "Xp": 100, "Description": "Play 1 Dodgeball game"},
            {"ObjectiveType": 302, "RequiredScore": 5, "Xp": 100, "Description": "Hit 5 players in Dodgeball"},
            {"ObjectiveType": 801, "RequiredScore": 1, "Xp": 100, "Description": "Play 1 Soccer game"},
        ],
    }
]
DEFAULT_OBJECTIVES_CONFIG_V1 = [
    [
        {"type": 301, "score": 1, "xp": 100},
        {"type": 302, "score": 5, "xp": 100},
        {"type": 801, "score": 1, "xp": 100},
    ],
    [
        {"type": 801, "score": 1, "xp": 100},
        {"type": 802, "score": 2, "xp": 100},
        {"type": 400, "score": 1, "xp": 100},
    ],
    [
        {"type": 201, "score": 1, "xp": 100},
        {"type": 202, "score": 3, "xp": 100},
        {"type": 302, "score": 3, "xp": 100},
    ],
    [
        {"type": 500, "score": 1, "xp": 100},
        {"type": 502, "score": 5, "xp": 100},
        {"type": 603, "score": 1, "xp": 100},
    ],
    [
        {"type": 701, "score": 1, "xp": 100},
        {"type": 502, "score": 5, "xp": 100},
        {"type": 501, "score": 1, "xp": 100},
    ],
    [
        {"type": 100, "score": 1, "xp": 100},
        {"type": 101, "score": 1, "xp": 100},
        {"type": 300, "score": 1, "xp": 100},
    ],
    [
        {"type": 400, "score": 1, "xp": 100},
        {"type": 402, "score": 5, "xp": 100},
        {"type": 200, "score": 1, "xp": 100},
    ],
]

app.config["MAX_CONTENT_LENGTH"] = MAX_REQUEST_BODY_BYTES


def ensure_dirs() -> None:
    shared.ensure_data_dir()
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)


def now_http_date() -> str:
    return format_datetime(datetime.now(timezone.utc), usegmt=True)


def image_meta_path(player_id: int) -> Path:
    return IMAGES_DIR / f"{player_id}.json"


def image_candidate_paths(player_id: int) -> list[Path]:
    return [
        IMAGES_DIR / f"{player_id}.png",
        IMAGES_DIR / f"{player_id}.jpg",
        IMAGES_DIR / f"{player_id}.jpeg",
        IMAGES_DIR / f"{player_id}.bin",
    ]


def detect_content_type(image_bytes: bytes, provided: str = "", filename: str = "") -> str:
    raw = (provided or "").split(";", 1)[0].strip().lower()
    lower_name = (filename or "").strip().lower()
    if raw in {"image/png", "image/jpeg", "image/jpg"}:
        return "image/jpeg" if raw == "image/jpg" else raw
    if lower_name.endswith(".png"):
        return "image/png"
    if lower_name.endswith(".jpg") or lower_name.endswith(".jpeg"):
        return "image/jpeg"
    if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if image_bytes.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    return ""


def load_image(player_id: int) -> tuple[bytes, str, str]:
    ensure_dirs()
    meta = {}
    meta_path = image_meta_path(player_id)
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}

    for candidate in image_candidate_paths(player_id):
        if candidate.exists():
            image_bytes = candidate.read_bytes()
            content_type = detect_content_type(image_bytes, str(meta.get("content_type") or ""), str(meta.get("filename") or candidate.name))
            if content_type:
                last_modified = str(meta.get("last_modified") or now_http_date())
                return image_bytes, content_type, last_modified

    return TRANSPARENT_PNG, "image/png", now_http_date()


def save_image(player_id: int, image_bytes: bytes, content_type: str, filename: str = "") -> dict[str, Any]:
    ensure_dirs()
    normalized = detect_content_type(image_bytes, content_type, filename)
    if normalized not in {"image/png", "image/jpeg"}:
        raise ValueError("unsupported image type")

    target = IMAGES_DIR / f"{player_id}{'.png' if normalized == 'image/png' else '.jpg'}"
    for candidate in image_candidate_paths(player_id):
        if candidate != target and candidate.exists():
            try:
                candidate.unlink()
            except Exception:
                pass

    target.write_bytes(image_bytes)
    meta = {
        "content_type": normalized,
        "filename": filename or target.name,
        "last_modified": now_http_date(),
    }
    image_meta_path(player_id).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta


def request_payload() -> dict[str, Any]:
    payload: dict[str, Any] = {}
    json_payload = request.get_json(silent=True)
    if isinstance(json_payload, dict):
        payload.update(json_payload)
    for mapping in (request.args, request.form):
        for key in mapping.keys():
            values = mapping.getlist(key)
            if not values:
                continue
            payload[key] = values[-1] if len(values) == 1 else values
    raw_text = request.get_data(cache=True, as_text=True).strip()
    if raw_text and not payload:
        try:
            parsed = json.loads(raw_text)
            if isinstance(parsed, dict):
                payload.update(parsed)
        except Exception:
            pass
    return payload


def request_id_list() -> list[int]:
    json_payload = request.get_json(silent=True)
    if isinstance(json_payload, list):
        return [shared.safe_int(item, 0) for item in json_payload]
    payload = request_payload()
    for key in ("ids", "Ids", "playerIds", "PlayerIds"):
        value = payload.get(key)
        if isinstance(value, list):
            return [shared.safe_int(item, 0) for item in value]
    return []


def parse_upload() -> tuple[bytes, str, str]:
    for field_name in ("image", "file", "avatar", "profileImage", "avatarImage"):
        if field_name in request.files:
            upload = request.files[field_name]
            return upload.read(), upload.mimetype or "application/octet-stream", upload.filename or ""
    raw = request.get_data(cache=True)
    return raw, request.content_type or "application/octet-stream", ""


def auth_required() -> bool:
    return REQUIRE_HTTP_AUTH


def player_payload_defaults(payload: dict[str, Any], default_platform: int, default_platform_id: int, forced_player_id: int | None = None) -> dict[str, Any]:
    normalized = dict(payload or {})
    normalized.setdefault("Platform", default_platform)
    normalized.setdefault("PlatformId", default_platform_id)
    if forced_player_id is not None and forced_player_id > 0:
        normalized.setdefault("Id", forced_player_id)
    return normalized


@app.before_request
def before_request() -> Any:
    shared.init_db()
    ensure_dirs()
    if request.content_length is not None and request.content_length > MAX_REQUEST_BODY_BYTES:
        return jsonify({"error": "payload too large"}), 413
    if request.path.startswith("/api/") and auth_required() and not shared.auth_header_valid(request.headers.get("Authorization")):
        return Response("Unauthorized", 401, {"WWW-Authenticate": 'Basic realm="Rec Room API"'})
    return None


@app.after_request
def after_request(response: Response) -> Response:
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Cache-Control", "no-store")
    try:
        shared.log_request(request.method, request.path, dict(request.args), response.status_code)
    except Exception:
        pass
    return response


@app.get("/")
def root() -> Any:
    return jsonify({"ok": True, "service": "rr23-2016-http"})


@app.get("/health")
def health() -> Any:
    return jsonify({"ok": True, "service": "rr23-2016-http"})


@app.get("/__debug/requests")
def debug_requests() -> Any:
    if not ENABLE_DEBUG_ENDPOINTS:
        return jsonify({"error": "not found"}), 404
    return jsonify(shared.list_recent_requests())


@app.get("/__debug/websockets")
def debug_websockets() -> Any:
    if not ENABLE_DEBUG_ENDPOINTS:
        return jsonify({"error": "not found"}), 404
    return jsonify(shared.list_ws_sessions())


@app.route("/api/players/v1/create", methods=["POST"])
def players_create() -> Any:
    payload = request_payload()
    platform = shared.parse_platform(payload.get("Platform", payload.get("platform", shared.DEFAULT_PLATFORM)))
    platform_id = shared.safe_int(payload.get("PlatformId", payload.get("platformId", payload.get("id", 0))), 0)
    player = shared.create_or_update_player(
        platform=platform,
        platform_id=platform_id,
        payload=player_payload_defaults(payload, platform, platform_id),
    )
    return jsonify(player), 201


@app.route("/api/players/v1/update/<int:player_id>", methods=["POST", "PUT", "PATCH"])
def players_update(player_id: int) -> Any:
    payload = request_payload()
    current = shared.get_player_by_id(player_id)
    fallback_platform = shared.safe_int((current or {}).get("Platform"), shared.DEFAULT_PLATFORM)
    fallback_platform_id = shared.safe_int((current or {}).get("PlatformId"), player_id)
    platform = shared.parse_platform(payload.get("Platform", payload.get("platform", fallback_platform)))
    platform_id = shared.safe_int(payload.get("PlatformId", payload.get("platformId", fallback_platform_id)), fallback_platform_id)
    player = shared.create_or_update_player(
        platform=platform,
        platform_id=platform_id,
        payload=player_payload_defaults(payload, platform, platform_id, forced_player_id=player_id),
    )
    return jsonify(player)


@app.route("/api/players/v1/verify/<int:player_id>", methods=["POST"])
def players_verify(player_id: int) -> Any:
    player = shared.get_player_by_id(player_id)
    if player is None:
        return jsonify({"Message": "Player not found."}), 404
    return jsonify({"Message": "Verification complete.", "Verified": True})


@app.route("/api/players/v1", methods=["GET", "POST", "PUT", "PATCH"])
@app.route("/api/players/v1/", methods=["GET", "POST", "PUT", "PATCH"])
@app.route("/api/players/v1/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH"])
def players_v1(subpath: str = "") -> Any:
    payload = request_payload()
    pieces = [piece for piece in subpath.split("/") if piece]
    player: dict[str, Any] | None = None
    platform = shared.parse_platform(payload.get("p", payload.get("platform", payload.get("Platform", shared.DEFAULT_PLATFORM))))
    platform_id = shared.safe_int(payload.get("id", payload.get("platformId", payload.get("PlatformId", 0))), 0)

    if len(pieces) == 1 and pieces[0].isdigit():
        player = shared.get_player_by_id(int(pieces[0]))
        if player is not None:
            platform = shared.safe_int(player.get("Platform"), platform)
            platform_id = shared.safe_int(player.get("PlatformId"), platform_id)
    elif len(pieces) >= 2 and pieces[0].lstrip("-").isdigit() and pieces[1].isdigit():
        platform = int(pieces[0])
        platform_id = int(pieces[1])
        player = shared.get_player_by_platform(platform, platform_id)
    else:
        player = shared.get_player_by_platform(platform, platform_id)

    if request.method == "GET":
        if player is None and os.environ.get("AUTO_CREATE_ON_GET", "true").strip().lower() in {"1", "true", "yes", "y"}:
            player = shared.create_or_update_player(platform=platform, platform_id=platform_id, payload=player_payload_defaults(payload, platform, platform_id))
        if player is None:
            return jsonify({"error": "player not found"}), 404
        return jsonify(player)

    player = shared.create_or_update_player(platform=platform, platform_id=platform_id, payload=player_payload_defaults(payload, platform, platform_id))
    status_code = 201 if request.method == "POST" else 200
    return jsonify(player), status_code


@app.route("/api/players/v1/list", methods=["POST"])
def players_list() -> Any:
    return Response(json.dumps(shared.list_players_by_ids(request_id_list())), mimetype="application/json")


@app.route("/api/players/v1/updateReputation/<int:player_id>", methods=["POST"])
def players_update_reputation(player_id: int) -> Any:
    payload = request_payload()
    return jsonify(shared.set_reputation(player_id, shared.safe_int(payload.get("reputation", payload.get("Reputation", 0)), 0)))


@app.route("/api/players/v1/objective/<int:player_id>", methods=["POST"])
def players_objective(player_id: int) -> Any:
    payload = request_payload()
    return jsonify(
        shared.apply_objective_completion(
            player_id,
            shared.safe_int(payload.get("objectiveType", payload.get("ObjectiveType", 0)), 0),
            shared.safe_int(payload.get("additionalXp", payload.get("AdditionalXp", 0)), 0),
            shared.parse_bool(payload.get("inParty", payload.get("InParty", False)), False),
        )
    )


@app.route("/api/settings/v1", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/api/settings/v1/", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/api/settings/v1/<int:player_id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def settings_v1(player_id: int = 0) -> Any:
    payload = request_payload()
    if player_id <= 0:
        player_id = shared.safe_int(payload.get("PlayerId", payload.get("playerId", payload.get("Id", payload.get("id", 0)))), 0)
    if request.method == "GET":
        return Response(json.dumps(shared.get_settings(player_id)), mimetype="application/json")
    entries = payload.get("settings") or payload.get("Settings") or payload
    if isinstance(entries, list):
        for item in entries:
            if not isinstance(item, dict):
                continue
            key = str(item.get("Key") or item.get("key") or "").strip()
            if not key:
                continue
            if request.method == "DELETE" or shared.parse_bool(item.get("Remove", item.get("remove", False)), False):
                shared.delete_setting(player_id, key)
            else:
                shared.upsert_setting(player_id, key, str(item.get("Value", item.get("value", ""))))
    elif isinstance(payload, dict):
        key = str(payload.get("Key") or payload.get("key") or "").strip()
        if key:
            if request.method == "DELETE" or shared.parse_bool(payload.get("Remove", payload.get("remove", False)), False):
                shared.delete_setting(player_id, key)
            else:
                shared.upsert_setting(player_id, key, str(payload.get("Value", payload.get("value", ""))))
    return jsonify({"ok": True, "playerId": player_id, "count": len(shared.get_settings(player_id))})


@app.route("/api/avatar/v1/<int:player_id>", methods=["GET", "POST", "PUT", "PATCH"])
def avatar_v1(player_id: int) -> Any:
    if request.method == "GET":
        return jsonify(shared.get_avatar(player_id))
    return jsonify(shared.set_avatar(player_id, request_payload()))


@app.route("/api/avatar/v1/set", methods=["POST", "PUT", "PATCH"])
def avatar_set() -> Any:
    payload = request_payload()
    player_id = shared.safe_int(payload.get("PlayerId", payload.get("playerId", payload.get("Id", payload.get("id", 0)))), 0)
    return jsonify(shared.set_avatar(player_id, payload))


@app.route("/api/avatar/v1/items/create", methods=["POST"])
def avatar_items_create() -> Any:
    payload = request_payload()
    player_id = shared.safe_int(payload.get("PlayerId", payload.get("playerId", payload.get("Id", payload.get("id", 0)))), 0)
    desc = str(payload.get("AvatarItemDesc", payload.get("avatarItemDesc", payload.get("Item", payload.get("item", "")))) or "").strip()
    if not desc:
        return jsonify({"error": "avatar item missing"}), 400
    return jsonify(shared.add_avatar_item(player_id, desc, shared.safe_int(payload.get("UnlockedLevel", payload.get("unlockedLevel", 1)), 1)))


@app.route("/api/avatar/v1/items/<int:player_id>", methods=["GET"])
@app.route("/api/avatar/v1/items/unlocked/<int:player_id>", methods=["GET"])
@app.route("/api/avatar/v1/unlocked/<int:player_id>", methods=["GET"])
def avatar_items_get(player_id: int) -> Any:
    return Response(json.dumps(shared.get_avatar_items(player_id)), mimetype="application/json")


@app.route("/api/avatar/v1/gifts/<int:player_id>", methods=["GET"])
def gifts_get(player_id: int) -> Any:
    return Response(json.dumps(shared.get_gift_packages(player_id)), mimetype="application/json")


@app.route("/api/avatar/v1/gifts/create/<int:player_id>", methods=["POST"])
def gifts_create(player_id: int) -> Any:
    payload = request_payload()
    return jsonify(shared.create_gift_package(player_id, str(payload.get("AvatarItemDesc", payload.get("avatarItemDesc", "")) or ""), shared.safe_int(payload.get("Xp", payload.get("xp", 0)), 0)))


@app.route("/api/avatar/v1/gifts/consume", methods=["POST"])
@app.route("/api/avatar/v1/gifts/consume/", methods=["POST"])
def gifts_consume() -> Any:
    payload = request_payload()
    player_id = shared.safe_int(payload.get("PlayerId", payload.get("playerId", 0)), 0)
    gift_id = shared.safe_int(payload.get("Id", payload.get("id", 0)), 0)
    if not shared.consume_gift_package(player_id, gift_id):
        return jsonify({"error": "gift not found"}), 404
    return jsonify({"ok": True, "Id": gift_id})


@app.route("/api/images/v1/profile/<int:player_id>", methods=["GET", "POST", "PUT"])
def profile_image(player_id: int) -> Any:
    if request.method == "GET":
        image_bytes, content_type, last_modified = load_image(player_id)
        incoming = request.headers.get("If-Modified-Since", "").strip()
        if incoming:
            try:
                if parsedate_to_datetime(incoming) >= parsedate_to_datetime(last_modified):
                    response = Response(status=304)
                    response.headers["LAST-MODIFIED"] = last_modified
                    response.headers["Last-Modified"] = last_modified
                    return response
            except Exception:
                pass
        response = Response(image_bytes, mimetype=content_type)
        response.headers["Content-Length"] = str(len(image_bytes))
        response.headers["LAST-MODIFIED"] = last_modified
        response.headers["Last-Modified"] = last_modified
        response.headers["Date"] = now_http_date()
        return response

    image_bytes, content_type, filename = parse_upload()
    if not image_bytes:
        return jsonify({"error": "image payload missing"}), 400
    if len(image_bytes) > MAX_REQUEST_BODY_BYTES:
        return jsonify({"error": "payload too large"}), 413
    try:
        meta = save_image(player_id, image_bytes, content_type, filename)
    except ValueError:
        return jsonify({"error": "unsupported image type", "allowed": ["image/png", "image/jpeg"]}), 415
    return jsonify({"ok": True, "playerId": player_id, **meta})


@app.route("/api/objectives/v1", methods=["GET"])
@app.route("/api/objectives/v1/", methods=["GET"])
@app.route("/api/objectives/v1/<path:subpath>", methods=["GET"])
@app.route("/api/dailyobjectives/v1", methods=["GET"])
@app.route("/api/dailyobjectives/v1/", methods=["GET"])
@app.route("/api/dailyobjectives/v1/<path:subpath>", methods=["GET"])
def objectives_v1(subpath: str = "") -> Any:
    return jsonify({"DateUtc": datetime.now(timezone.utc).date().isoformat(), "DailyObjectives": DEFAULT_OBJECTIVES})


@app.route("/api/config/v1/objectives", methods=["GET"])
@app.route("/api/config/v1/objectives/", methods=["GET"])
def objectives_config() -> Any:
    return Response(json.dumps(DEFAULT_OBJECTIVES_CONFIG_V1), mimetype="application/json")


@app.route("/api/config/v1/motd", methods=["GET", "POST", "PUT"])
@app.route("/api/config/v1/motd/", methods=["GET", "POST", "PUT"])
def motd() -> Any:
    if request.method == "GET":
        return Response(shared.get_motd(), mimetype="text/plain")
    payload = request_payload()
    value = str(payload.get("motd", payload.get("message", payload.get("text", request.get_data(cache=True, as_text=True)))) or shared.DEFAULT_MOTD_TEXT)
    return jsonify({"ok": True, "motd": shared.set_motd(value)})


@app.route("/api/presence/v1/list", methods=["POST", "GET"])
def presence_list() -> Any:
    ids = request_id_list()
    if not ids:
        payload = request_payload()
        single_id = shared.safe_int(payload.get("PlayerId", payload.get("playerId", payload.get("Id", payload.get("id", 0)))), 0)
        if single_id > 0:
            ids = [single_id]
    return Response(json.dumps(shared.list_presence(ids)), mimetype="application/json")


@app.route("/api/presence/v1/<int:player_id>", methods=["GET", "POST"])
def presence_player(player_id: int) -> Any:
    if request.method == "GET":
        presence = shared.get_presence(player_id)
        return Response("null" if presence is None else json.dumps(presence), mimetype="application/json")
    return jsonify(shared.set_presence(player_id, request_payload()))


@app.route("/api/gamesessions/v1", methods=["GET"])
@app.route("/api/gamesessions/v1/", methods=["GET"])
def game_sessions() -> Any:
    return Response(json.dumps(shared.get_game_sessions(str(request.args.get("v", "") or ""))), mimetype="application/json")


@app.route("/api/gamesessions/v1/<path:session_id>", methods=["GET"])
def game_session(session_id: str) -> Any:
    session = shared.get_game_session(session_id)
    if session is None:
        return Response("{}", mimetype="application/json", status=404)
    return Response(json.dumps(session), mimetype="application/json")


@app.route("/api/messages/v1/get/<int:player_id>", methods=["GET"])
@app.route("/api/messages/v1/get", methods=["GET", "POST"])
def messages_get(player_id: int | None = None) -> Any:
    if player_id is None:
        payload = request_payload()
        player_id = shared.safe_int(payload.get("PlayerId", payload.get("playerId", payload.get("Id", payload.get("id", 0)))), 0)
    return Response(json.dumps(shared.get_messages_for_player(shared.safe_int(player_id, 0))), mimetype="application/json")


@app.route("/api/messages/v1/send", methods=["POST"])
def messages_send() -> Any:
    payload = request_payload()
    message = shared.create_message(
        shared.safe_int(payload.get("FromPlayerId", payload.get("fromPlayerId", 0)), 0),
        shared.safe_int(payload.get("ToPlayerId", payload.get("toPlayerId", 0)), 0),
        shared.safe_int(payload.get("Type", payload.get("type", 0)), 0),
        str(payload.get("Data", payload.get("data", "")) or ""),
    )
    return Response(json.dumps({k: v for k, v in message.items() if k != "ToPlayerId"}), mimetype="application/json")


@app.route("/api/messages/v1/delete", methods=["POST"])
@app.route("/api/messages/v1/delete/<int:message_id>", methods=["POST", "DELETE"])
def messages_delete(message_id: int | None = None) -> Any:
    payload = request_payload()
    resolved_message_id = message_id if message_id is not None else shared.safe_int(payload.get("Id", payload.get("id", 0)), 0)
    if resolved_message_id <= 0 or not shared.delete_message(resolved_message_id):
        return jsonify({"error": "message not found"}), 404
    return jsonify({"ok": True, "Id": resolved_message_id})


@app.route("/api/relationships/v1/get/<int:player_id>", methods=["GET"])
@app.route("/api/relationships/v1/get", methods=["GET", "POST"])
def relationships_get(player_id: int | None = None) -> Any:
    if player_id is None:
        payload = request_payload()
        player_id = shared.safe_int(payload.get("PlayerId", payload.get("playerId", payload.get("Id", payload.get("id", 0)))), 0)
    return Response(json.dumps(shared.get_relationships(shared.safe_int(player_id, 0))), mimetype="application/json")


@app.route("/api/relationships/v1/<action>", methods=["GET"])
def relationships_action(action: str) -> Any:
    id1 = shared.safe_int(request.args.get("id1", 0), 0)
    id2 = shared.safe_int(request.args.get("id2", 0), 0)
    return Response(json.dumps(shared.apply_relationship_action(action, id1, id2)), mimetype="application/json")


@app.route("/api/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def api_fallback(subpath: str) -> Any:
    return jsonify({"ok": True, "path": f"/api/{subpath}", "method": request.method, "note": "Fallback response"})


if __name__ == "__main__":
    shared.init_db()
    ensure_dirs()
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
