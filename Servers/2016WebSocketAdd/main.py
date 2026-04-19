from __future__ import annotations

import base64
import json
import os
import threading
from datetime import date, datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, request

import rr23_shared as shared
import rr23_notifier as notifier

app = Flask(__name__)
notifier.start_background_workers()

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
            {"ObjectiveType": 301, "RequiredScore": 1, "Xp": 100},
            {"ObjectiveType": 300, "RequiredScore": 1, "Xp": 100},
            {"ObjectiveType": 801, "RequiredScore": 1, "Xp": 100},
        ],
    }
]
DEFAULT_OBJECTIVES_CONFIG_V1 = [
    [
        {"type": 301, "score": 1, "xp": 100},
        {"type": 300, "score": 1, "xp": 100},
        {"type": 801, "score": 1, "xp": 100},
    ],
    [
        {"type": 801, "score": 1, "xp": 100},
        {"type": 800, "score": 1, "xp": 100},
        {"type": 301, "score": 1, "xp": 100},
    ],
    [
        {"type": 201, "score": 1, "xp": 100},
        {"type": 200, "score": 1, "xp": 100},
        {"type": 400, "score": 1, "xp": 100},
    ],
    [
        {"type": 500, "score": 1, "xp": 100},
        {"type": 501, "score": 1, "xp": 100},
        {"type": 301, "score": 1, "xp": 100},
    ],
    [
        {"type": 601, "score": 1, "xp": 100},
        {"type": 600, "score": 1, "xp": 100},
        {"type": 801, "score": 1, "xp": 100},
    ],
    [
        {"type": 701, "score": 1, "xp": 100},
        {"type": 700, "score": 1, "xp": 100},
        {"type": 500, "score": 1, "xp": 100},
    ],
    [
        {"type": 400, "score": 1, "xp": 100},
        {"type": 401, "score": 1, "xp": 100},
        {"type": 201, "score": 1, "xp": 100},
    ],
]

app.config["MAX_CONTENT_LENGTH"] = MAX_REQUEST_BODY_BYTES

CLIENT_LOCAL_PLAYER_IDS: dict[str, int] = {}
LAST_LOCAL_PLAYER_ID: int = 0
RUNTIME_INIT_LOCK = threading.Lock()
RUNTIME_READY = False


def ensure_runtime_ready() -> None:
    global RUNTIME_READY
    if RUNTIME_READY:
        return
    with RUNTIME_INIT_LOCK:
        if RUNTIME_READY:
            return
        shared.init_db()
        ensure_dirs()
        RUNTIME_READY = True


def client_identity_key() -> str:
    forwarded_for = request.headers.get("X-Forwarded-For", "").split(",", 1)[0].strip()
    remote = forwarded_for or (request.remote_addr or "")
    user_agent = request.headers.get("User-Agent", "")
    return f"{remote}|{user_agent}"


def _header_local_player_id() -> int:
    for header_name in (
        "X-Rec-Room-Profile",
        "X-RecRoom-Profile",
        "X-Rec-Room-PlayerId",
        "X-RecRoom-PlayerId",
    ):
        header_value = shared.safe_int(request.headers.get(header_name), 0)
        if header_value > 0:
            return header_value
    return 0


def remember_local_player_id(player_id: int) -> None:
    global LAST_LOCAL_PLAYER_ID
    player_id = shared.safe_int(player_id, 0)
    if player_id <= 0:
        return
    LAST_LOCAL_PLAYER_ID = player_id
    CLIENT_LOCAL_PLAYER_IDS[client_identity_key()] = player_id


def resolve_local_player_id(payload: dict[str, Any] | None = None, default: int = 0, *, allow_generic_id: bool = False) -> int:
    payload = payload if isinstance(payload, dict) else {}

    header_player_id = _header_local_player_id()
    if header_player_id > 0:
        return header_player_id

    for key in ("PlayerId", "playerId", "PlayerID", "playerID", "ProfileId", "profileId"):
        value = shared.safe_int(payload.get(key), 0)
        if value > 0:
            return value

    if allow_generic_id:
        for key in ("Id", "id"):
            value = shared.safe_int(payload.get(key), 0)
            if value > 0:
                return value

    mapped = shared.safe_int(CLIENT_LOCAL_PLAYER_IDS.get(client_identity_key()), 0)
    if mapped > 0:
        return mapped
    if LAST_LOCAL_PLAYER_ID > 0:
        return LAST_LOCAL_PLAYER_ID
    return shared.safe_int(default, 0)


def profile_response_2016(player: dict[str, Any]) -> dict[str, Any]:
    return {
        "Id": shared.safe_int(player.get("Id"), 0),
        "Username": str(player.get("Username") or player.get("DisplayName") or shared.DEFAULT_PLAYER_NAME),
        "DisplayName": str(player.get("DisplayName") or player.get("Username") or shared.DEFAULT_PLAYER_NAME),
        "XP": max(0, shared.safe_int(player.get("XP"), 0)),
        "XpRequiredToLevelUp": max(0, shared.safe_int(player.get("XpRequiredToLevelUp"), 0)),
        "Level": max(1, shared.safe_int(player.get("Level"), 1)),
        "Reputation": shared.safe_int(player.get("Reputation"), 0),
        "Verified": bool(player.get("Verified", True)),
        "Developer": bool(player.get("Developer", False)),
    }


def config_table_entries() -> list[dict[str, str]]:
    return [
        {"Key": "PlayerCanSave", "Value": "true"},
        {"Key": "MatchmakingEnabled", "Value": "true"},
        {"Key": "DormRoomEnabled", "Value": "true"},
        {"Key": "NotificationsEnabled", "Value": "true"},
    ]


def config_v2_payload() -> dict[str, Any]:
    return {
        "MessageOfTheDay": shared.get_motd(),
        "MatchmakingParams": {
            "PreferFullRoomsFrequency": 0.35,
            "PreferEmptyRoomsFrequency": 0.15,
        },
        "DailyObjectives": [
            [
                {
                    "type": shared.safe_int(item.get("type"), 0),
                    "score": shared.safe_int(item.get("score"), 0),
                }
                for item in objective_group
            ]
            for objective_group in DEFAULT_OBJECTIVES_CONFIG_V1
        ],
        "ConfigTable": config_table_entries(),
    }


def get_or_create_player_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    platform = shared.parse_platform(payload.get("Platform", payload.get("platform", payload.get("p", shared.DEFAULT_PLATFORM))))
    platform_id = shared.safe_int(payload.get("PlatformId", payload.get("platformId", payload.get("id", payload.get("Id", 0)))), 0)
    if platform_id <= 0:
        platform_id = 1
    player = shared.create_or_update_player(
        platform=platform,
        platform_id=platform_id,
        payload=player_payload_defaults(payload, platform, platform_id),
    )
    remember_local_player_id(shared.safe_int(player.get("Id"), 0))
    return player


def ensure_dirs() -> None:
    shared.ensure_data_dir()
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)


def now_http_date() -> str:
    return format_datetime(datetime.now(timezone.utc), usegmt=True)


def objective_day_index(target_date: date | datetime | None = None) -> int:
    if target_date is None:
        current_date = datetime.now(timezone.utc).date()
    elif isinstance(target_date, datetime):
        current_date = target_date.date()
    else:
        current_date = target_date
    return (current_date.weekday() + 1) % 7


def daily_objectives_for_date(target_date: date | datetime | None = None) -> dict[str, Any]:
    if target_date is None:
        current_date = datetime.now(timezone.utc).date()
    elif isinstance(target_date, datetime):
        current_date = target_date.date()
    else:
        current_date = target_date
    if not DEFAULT_OBJECTIVES_CONFIG_V1:
        return DEFAULT_OBJECTIVES[0]

    config = DEFAULT_OBJECTIVES_CONFIG_V1[objective_day_index(current_date)]
    return {
        "Date": current_date.isoformat(),
        "Objectives": [
            {
                "ObjectiveType": shared.safe_int(item.get("type"), 0),
                "RequiredScore": shared.safe_int(item.get("score"), 0),
                "Xp": shared.safe_int(item.get("xp"), 0),
            }
            for item in config
        ],
    }


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


def request_payload(include_query: bool = True) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    json_payload = request.get_json(silent=True)
    if isinstance(json_payload, dict):
        payload.update(json_payload)

    mappings = [request.form]
    if include_query:
        mappings.insert(0, request.args)

    for mapping in mappings:
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
    if not RUNTIME_READY:
        ensure_runtime_ready()
    if request.content_length is not None and request.content_length > MAX_REQUEST_BODY_BYTES:
        return jsonify({"error": "payload too large"}), 413
    if request.path.startswith("/api/") and auth_required() and not shared.auth_header_valid(request.headers.get("Authorization")):
        return Response("Unauthorized", 401, {"WWW-Authenticate": 'Basic realm="Rec Room API"'})
    return None


@app.after_request
def after_request(response: Response) -> Response:
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Cache-Control", "no-store")
    request_note = response.headers.pop('X-RR-Log-Note', '')
    try:
        shared.log_request(request.method, request.path, dict(request.args), response.status_code, request_note)
    except Exception:
        pass
    try:
        notifier.maybe_emit_periodic_snapshots()
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


@app.route("/api/players/v1/getorcreate", methods=["POST"])
@app.route("/api/players/v1/getorcreate/", methods=["POST"])
def players_get_or_create_v1() -> Any:
    player = get_or_create_player_from_payload(request_payload())
    return jsonify(profile_response_2016(player))


@app.route("/api/players/v2", methods=["GET", "POST", "PUT", "PATCH"])
@app.route("/api/players/v2/", methods=["GET", "POST", "PUT", "PATCH"])
def players_v2_root() -> Any:
    payload = request_payload()
    platform = shared.parse_platform(payload.get("p", payload.get("platform", payload.get("Platform", shared.DEFAULT_PLATFORM))))
    platform_id = shared.safe_int(payload.get("id", payload.get("platformId", payload.get("PlatformId", 0))), 0)
    player = shared.get_player_by_platform(platform, platform_id) if platform_id > 0 else None
    if player is None and request.method != "GET":
        player = get_or_create_player_from_payload(payload)
    elif player is None:
        return jsonify({"error": "player not found"}), 404
    remember_local_player_id(shared.safe_int(player.get("Id"), 0))
    return jsonify(profile_response_2016(player))


@app.route("/api/players/v2/updateReputation", methods=["POST"])
@app.route("/api/players/v2/updateReputation/", methods=["POST"])
def players_v2_update_reputation() -> Any:
    payload = request_payload()
    player_id = resolve_local_player_id(payload, allow_generic_id=True)
    player = shared.get_player_by_id(player_id)
    if player is None:
        player = get_or_create_player_from_payload({"Id": player_id or 1, **payload})
        player_id = shared.safe_int(player.get("Id"), 0)
    reputation_delta = shared.safe_int(payload.get("reputationDelta", payload.get("ReputationDelta", 0)), 0)
    current_reputation = shared.safe_int(player.get("Reputation"), 0)
    updated = shared.set_reputation(player_id, current_reputation - reputation_delta)
    remember_local_player_id(player_id)
    return jsonify(profile_response_2016(updated))


@app.route("/api/players/v2/verify", methods=["POST"])
@app.route("/api/players/v2/verify/", methods=["POST"])
def players_v2_verify() -> Any:
    payload = request_payload()
    email = str(payload.get("Email", payload.get("email", shared.DEFAULT_VERIFIED_EMAIL)) or shared.DEFAULT_VERIFIED_EMAIL)
    return jsonify({"Message": f"Verification email queued for {email}."})


@app.route("/api/players/v2/objective", methods=["POST"])
@app.route("/api/players/v2/objective/", methods=["POST"])
def players_v2_objective() -> Any:
    payload = request_payload()
    player_id = resolve_local_player_id(payload, allow_generic_id=True)
    remember_local_player_id(player_id)
    return jsonify(
        shared.apply_objective_completion(
            player_id,
            shared.safe_int(payload.get("objectiveType", payload.get("ObjectiveType", 0)), 0),
            shared.safe_int(payload.get("additionalXp", payload.get("AdditionalXp", 0)), 0),
            shared.parse_bool(payload.get("inParty", payload.get("InParty", False)), False),
        )
    )


@app.route("/api/config/v2", methods=["GET"])
@app.route("/api/config/v2/", methods=["GET"])
def config_v2() -> Any:
    return Response(json.dumps(config_v2_payload()), mimetype="application/json")


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
    remember_local_player_id(shared.safe_int(player.get("Id"), 0))
    return jsonify(profile_response_2016(player)), 201


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
    remember_local_player_id(shared.safe_int(player.get("Id"), 0))
    return jsonify(profile_response_2016(player))


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
        allow_auto_create = os.environ.get("AUTO_CREATE_ON_GET", "true").strip().lower() in {"1", "true", "yes", "y"}
        if player is None and allow_auto_create and not (len(pieces) == 1 and pieces[0].isdigit()) and platform_id > 0:
            player = shared.create_or_update_player(platform=platform, platform_id=platform_id, payload=player_payload_defaults(payload, platform, platform_id))
        if player is None:
            return jsonify({"error": "player not found"}), 404
        remember_local_player_id(shared.safe_int(player.get("Id"), 0))
        return jsonify(profile_response_2016(player))

    player = shared.create_or_update_player(platform=platform, platform_id=platform_id, payload=player_payload_defaults(payload, platform, platform_id))
    remember_local_player_id(shared.safe_int(player.get("Id"), 0))
    status_code = 201 if request.method == "POST" else 200
    return jsonify(profile_response_2016(player)), status_code


@app.route("/api/players/v1/list", methods=["POST"])
def players_list() -> Any:
    players = [profile_response_2016(player) for player in shared.list_players_by_ids(request_id_list())]
    return Response(json.dumps(players), mimetype="application/json")


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


def _settings_action_from_subpath(subpath: str = "") -> str:
    normalized = subpath.strip("/").lower()
    return normalized if normalized in {"set", "remove"} else ""


def _apply_settings_mutation(player_id: int, payload: dict[str, Any], subpath: str = "") -> dict[str, Any]:
    action = _settings_action_from_subpath(subpath)
    should_remove_all = request.method == "DELETE" or action == "remove"
    entries = payload.get("settings") or payload.get("Settings") or payload

    def apply_entry(item: dict[str, Any], fallback_key: str = "") -> None:
        key = str(item.get("Key") or item.get("key") or fallback_key or "").strip()
        if not key:
            return
        should_remove = should_remove_all or shared.parse_bool(item.get("Remove", item.get("remove", False)), False)
        if should_remove:
            shared.delete_setting(player_id, key)
            return
        shared.upsert_setting(player_id, key, str(item.get("Value", item.get("value", item.get("SettingValue", "")))))

    if isinstance(entries, list):
        for item in entries:
            if isinstance(item, dict):
                apply_entry(item)
    elif isinstance(entries, dict):
        fallback_key = "" if action else subpath.rsplit("/", 1)[-1].strip()
        apply_entry(entries, fallback_key)

    remember_local_player_id(player_id)
    return {"ok": True, "playerId": player_id, "count": len(shared.get_settings(player_id))}


@app.route("/api/settings/v2", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/api/settings/v2/", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/api/settings/v2/set", methods=["POST", "PUT", "PATCH"])
@app.route("/api/settings/v2/set/", methods=["POST", "PUT", "PATCH"])
@app.route("/api/settings/v2/remove", methods=["POST", "PUT", "PATCH", "DELETE"])
@app.route("/api/settings/v2/remove/", methods=["POST", "PUT", "PATCH", "DELETE"])
@app.route("/api/settings/v2/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def settings_v2(subpath: str = "") -> Any:
    payload = request_payload()
    player_id = resolve_local_player_id(payload, allow_generic_id=True)
    if request.method == "GET":
        return Response(json.dumps(shared.get_settings(player_id)), mimetype="application/json")
    return jsonify(_apply_settings_mutation(player_id, payload, subpath))


@app.route("/api/settings/v1", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/api/settings/v1/", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/api/settings/v1/<int:player_id>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def settings_v1(player_id: int = 0) -> Any:
    payload = request_payload()
    if player_id <= 0:
        player_id = resolve_local_player_id(payload, allow_generic_id=True)
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


@app.route("/api/avatar/v2", methods=["GET"])
@app.route("/api/avatar/v2/", methods=["GET"])
def avatar_v2_get() -> Any:
    player_id = resolve_local_player_id(request_payload(), allow_generic_id=True)
    remember_local_player_id(player_id)
    return jsonify(shared.get_avatar(player_id))


@app.route("/api/avatar/v2/set", methods=["POST", "PUT", "PATCH"])
@app.route("/api/avatar/v2/set/", methods=["POST", "PUT", "PATCH"])
def avatar_v2_set() -> Any:
    payload = request_payload()
    player_id = resolve_local_player_id(payload, allow_generic_id=True)
    remember_local_player_id(player_id)
    return jsonify(shared.set_avatar(player_id, payload))


@app.route("/api/avatar/v2/gifts", methods=["GET"])
@app.route("/api/avatar/v2/gifts/", methods=["GET"])
def avatar_v2_gifts() -> Any:
    player_id = resolve_local_player_id(request_payload(), allow_generic_id=True)
    remember_local_player_id(player_id)
    return Response(json.dumps(shared.get_gift_packages(player_id)), mimetype="application/json")


@app.route("/api/avatar/v2/gifts/create", methods=["POST"])
@app.route("/api/avatar/v2/gifts/create/", methods=["POST"])
def avatar_v2_gifts_create() -> Any:
    payload = request_payload()
    player_id = resolve_local_player_id(payload, allow_generic_id=True)
    remember_local_player_id(player_id)
    return jsonify(shared.create_gift_package(player_id, str(payload.get("AvatarItemDesc", payload.get("avatarItemDesc", "")) or ""), shared.safe_int(payload.get("Xp", payload.get("xp", 0)), 0)))


@app.route("/api/avatar/v2/gifts/consume", methods=["POST"])
@app.route("/api/avatar/v2/gifts/consume/", methods=["POST"])
def avatar_v2_gifts_consume() -> Any:
    payload = request_payload()
    player_payload = {k: v for k, v in payload.items() if k not in {"Id", "id"}}
    player_id = resolve_local_player_id(player_payload, allow_generic_id=True)
    gift_id = shared.safe_int(payload.get("Id", payload.get("id", 0)), 0)
    remember_local_player_id(player_id)
    if not shared.consume_gift_package(player_id, gift_id):
        return jsonify({"error": "gift not found"}), 404
    return jsonify({"ok": True, "Id": gift_id})

@app.route("/api/versioncheck/v1", methods=["GET"])
@app.route("/api/versioncheck/v1/", methods=["GET"])
def versioncheck_v1() -> Any:
    return Response("", status=204)


@app.route("/api/tournament", methods=["GET"])
@app.route("/api/tournament/", methods=["GET"])
def tournament_status() -> Any:
    return Response("", mimetype="application/json")


@app.route("/api/tournament/forfeit", methods=["GET", "POST"])
@app.route("/api/tournament/forfeit/", methods=["GET", "POST"])
def tournament_forfeit() -> Any:
    return jsonify({"ok": True})

@app.route("/api/avatar/v2/items", methods=["GET"])
@app.route("/api/avatar/v2/items/", methods=["GET"])
def avatar_v2_items() -> Any:
    player_id = resolve_local_player_id(request_payload(), allow_generic_id=True)
    remember_local_player_id(player_id)
    return Response(json.dumps(shared.get_avatar_items(player_id)), mimetype="application/json")


@app.route("/api/avatar/v2/unlocked", methods=["GET"])
@app.route("/api/avatar/v2/unlocked/", methods=["GET"])
@app.route("/api/avatar/v2/items/unlocked", methods=["GET"])
@app.route("/api/avatar/v2/items/unlocked/", methods=["GET"])
@app.route("/api/avatar/v2/list", methods=["GET"])
@app.route("/api/avatar/v2/list/", methods=["GET"])
def avatar_v2_items_aliases() -> Any:
    player_id = resolve_local_player_id(request_payload(), allow_generic_id=True)
    remember_local_player_id(player_id)
    return Response(json.dumps(shared.get_avatar_items(player_id)), mimetype="application/json")


@app.route("/api/avatar/v3/items", methods=["GET"])
@app.route("/api/avatar/v3/items/", methods=["GET"])
@app.route("/api/avatar/v3/unlocked", methods=["GET"])
@app.route("/api/avatar/v3/unlocked/", methods=["GET"])
@app.route("/api/avatar/v3/list", methods=["GET"])
@app.route("/api/avatar/v3/list/", methods=["GET"])
def avatar_v3_items() -> Any:
    player_id = resolve_local_player_id(request_payload(), allow_generic_id=True)
    remember_local_player_id(player_id)
    return Response(json.dumps(shared.get_avatar_items(player_id)), mimetype="application/json")


@app.route("/api/avatar/v3/<path:subpath>", methods=["GET"])
def avatar_v3_fallback(subpath: str) -> Any:
    normalized = subpath.strip("/").lower()
    player_id = resolve_local_player_id(request_payload(), allow_generic_id=True)
    remember_local_player_id(player_id)
    if any(token in normalized for token in ("item", "unlock", "list")):
        return Response(json.dumps(shared.get_avatar_items(player_id)), mimetype="application/json")
    return Response(json.dumps(shared.get_avatar(player_id)), mimetype="application/json")


@app.route("/api/avatar/v2/<path:subpath>", methods=["GET"])
def avatar_v2_fallback(subpath: str) -> Any:
    normalized = subpath.strip("/").lower()
    player_id = resolve_local_player_id(request_payload(), allow_generic_id=True)
    remember_local_player_id(player_id)
    if any(token in normalized for token in ("item", "unlock", "list")):
        return Response(json.dumps(shared.get_avatar_items(player_id)), mimetype="application/json")
    return Response(json.dumps(shared.get_avatar(player_id)), mimetype="application/json")


@app.route("/api/avatar/v1/<int:player_id>", methods=["GET", "POST", "PUT", "PATCH"])
@app.route("/api/avatar/v1/<int:player_id>/", methods=["GET", "POST", "PUT", "PATCH"])
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
@app.route("/api/avatar/v1/items/<int:player_id>/", methods=["GET"])
@app.route("/api/avatar/v1/items/unlocked/<int:player_id>", methods=["GET"])
@app.route("/api/avatar/v1/items/unlocked/<int:player_id>/", methods=["GET"])
@app.route("/api/avatar/v1/unlocked/<int:player_id>", methods=["GET"])
@app.route("/api/avatar/v1/unlocked/<int:player_id>/", methods=["GET"])
def avatar_items_get(player_id: int) -> Any:
    return Response(json.dumps(shared.get_avatar_items(player_id)), mimetype="application/json")


@app.route("/api/avatar/v1/gifts/<int:player_id>", methods=["GET"])
@app.route("/api/avatar/v1/gifts/<int:player_id>/", methods=["GET"])
def gifts_get(player_id: int) -> Any:
    return Response(json.dumps(shared.get_gift_packages(player_id)), mimetype="application/json")


@app.route("/api/avatar/v1/gifts/create/<int:player_id>", methods=["POST"])
@app.route("/api/avatar/v1/gifts/create/<int:player_id>/", methods=["POST"])
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


@app.route("/api/images/v2/profile", methods=["POST", "PUT"])
@app.route("/api/images/v2/profile/", methods=["POST", "PUT"])
def profile_image_v2() -> Any:
    payload = request_payload()
    player_id = resolve_local_player_id(payload, allow_generic_id=True)
    image_bytes, content_type, filename = parse_upload()
    if not image_bytes:
        return jsonify({"error": "image payload missing"}), 400
    if len(image_bytes) > MAX_REQUEST_BODY_BYTES:
        return jsonify({"error": "payload too large"}), 413
    try:
        meta = save_image(player_id, image_bytes, content_type, filename)
    except ValueError:
        return jsonify({"error": "unsupported image type", "allowed": ["image/png", "image/jpeg"]}), 415
    remember_local_player_id(player_id)
    return jsonify({"ok": True, "playerId": player_id, **meta})


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
    today = datetime.now(timezone.utc).date().isoformat()
    return jsonify({"DateUtc": today, "DailyObjectives": [daily_objectives_for_date()]})


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


@app.route("/api/presence/v2", methods=["POST", "PUT", "PATCH"])
@app.route("/api/presence/v2/", methods=["POST", "PUT", "PATCH"])
def presence_v2() -> Any:
    payload = request_payload()
    player_id = resolve_local_player_id(payload, allow_generic_id=True)
    payload = {**payload, "PlayerId": player_id}
    remember_local_player_id(player_id)
    presence = shared.set_presence(player_id, payload)
    try:
        notifier.emit_presence_snapshot_now()
    except Exception:
        pass
    return jsonify(presence)


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
@app.route("/api/presence/v1/<int:player_id>/", methods=["GET", "POST"])
def presence_player(player_id: int) -> Any:
    if request.method == "GET":
        presence = shared.get_presence(player_id)
        return Response("null" if presence is None else json.dumps(presence), mimetype="application/json")
    presence = shared.set_presence(player_id, request_payload())
    try:
        notifier.emit_presence_snapshot_now()
    except Exception:
        pass
    return jsonify(presence)


@app.route("/api/gamesessions/v1", methods=["GET"])
@app.route("/api/gamesessions/v1/", methods=["GET"])
def game_sessions() -> Any:
    return Response(json.dumps(shared.get_game_sessions(str(request.args.get("v", "") or ""))), mimetype="application/json")


@app.route("/api/gamesessions/v1/<path:session_id>", methods=["GET"])
@app.route("/api/gamesessions/v1/<path:session_id>/", methods=["GET"])
def game_session(session_id: str) -> Any:
    session = shared.get_game_session(session_id)
    if session is None:
        return Response("{}", mimetype="application/json", status=404)
    return Response(json.dumps(session), mimetype="application/json")


@app.route("/api/messages/v2/get", methods=["GET", "POST"])
@app.route("/api/messages/v2/get/", methods=["GET", "POST"])
def messages_v2_get() -> Any:
    player_id = resolve_local_player_id(request_payload(), allow_generic_id=True)
    remember_local_player_id(player_id)
    return Response(json.dumps(shared.get_messages_for_player(player_id)), mimetype="application/json")


@app.route("/api/messages/v2/send", methods=["POST"])
@app.route("/api/messages/v2/send/", methods=["POST"])
def messages_v2_send() -> Any:
    payload = request_payload()
    from_player_id = resolve_local_player_id(payload, allow_generic_id=True)
    to_player_id = shared.safe_int(payload.get("ToPlayerId", payload.get("toPlayerId", 0)), 0)
    remember_local_player_id(from_player_id)
    message = shared.create_message(from_player_id, to_player_id, shared.safe_int(payload.get("Type", payload.get("type", 0)), 0), str(payload.get("Data", payload.get("data", "")) or ""))
    return Response(json.dumps({k: v for k, v in message.items() if k != "ToPlayerId"}), mimetype="application/json")


@app.route("/api/messages/v2/delete", methods=["POST", "DELETE"])
@app.route("/api/messages/v2/delete/", methods=["POST", "DELETE"])
@app.route("/api/messages/v2/delete/<int:message_id>", methods=["POST", "DELETE"])
@app.route("/api/messages/v2/delete/<int:message_id>/", methods=["POST", "DELETE"])
def messages_v2_delete(message_id: int | None = None) -> Any:
    payload = request_payload()
    resolved_message_id = message_id if message_id is not None else shared.safe_int(payload.get("Id", payload.get("id", 0)), 0)
    if resolved_message_id <= 0 or not shared.delete_message(resolved_message_id):
        return jsonify({"error": "message not found"}), 404
    return jsonify({"ok": True, "Id": resolved_message_id})


@app.route("/api/messages/v1/get/<int:player_id>", methods=["GET"])
@app.route("/api/messages/v1/get", methods=["GET", "POST"])
def messages_get(player_id: int | None = None) -> Any:
    if player_id is None:
        payload = request_payload()
        player_id = resolve_local_player_id(payload, allow_generic_id=True)
    return Response(json.dumps(shared.get_messages_for_player(shared.safe_int(player_id, 0))), mimetype="application/json")


@app.route("/api/messages/v1/send", methods=["POST"])
@app.route("/api/messages/v1/send/", methods=["POST"])
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
@app.route("/api/messages/v1/delete/", methods=["POST"])
@app.route("/api/messages/v1/delete/<int:message_id>", methods=["POST", "DELETE"])
@app.route("/api/messages/v1/delete/<int:message_id>/", methods=["POST", "DELETE"])
def messages_delete(message_id: int | None = None) -> Any:
    payload = request_payload()
    resolved_message_id = message_id if message_id is not None else shared.safe_int(payload.get("Id", payload.get("id", 0)), 0)
    if resolved_message_id <= 0 or not shared.delete_message(resolved_message_id):
        return jsonify({"error": "message not found"}), 404
    return jsonify({"ok": True, "Id": resolved_message_id})


@app.route("/api/relationships/v2/get", methods=["GET", "POST"])
@app.route("/api/relationships/v2/get/", methods=["GET", "POST"])
def relationships_v2_get() -> Any:
    player_id = resolve_local_player_id(request_payload(include_query=False), allow_generic_id=True)
    remember_local_player_id(player_id)
    return Response(json.dumps(shared.get_relationships(player_id)), mimetype="application/json")


@app.route("/api/relationships/v2/<action>", methods=["GET"])
@app.route("/api/relationships/v2/<action>/", methods=["GET"])
def relationships_v2_action(action: str) -> Any:
    payload = request_payload(include_query=False)
    local_player_id = resolve_local_player_id(payload, allow_generic_id=True)
    other_player_id = shared.safe_int(request.args.get("id", payload.get("TargetPlayerId", payload.get("targetPlayerId", 0))), 0)
    if local_player_id <= 0 or other_player_id <= 0 or local_player_id == other_player_id:
        return jsonify({"error": "invalid relationship target"}), 400
    remember_local_player_id(local_player_id)
    return Response(json.dumps(shared.apply_relationship_action(action, local_player_id, other_player_id)), mimetype="application/json")


@app.route("/api/relationships/v1/get/<int:player_id>", methods=["GET"])
@app.route("/api/relationships/v1/get", methods=["GET", "POST"])
def relationships_get(player_id: int | None = None) -> Any:
    if player_id is None:
        payload = request_payload()
        player_id = resolve_local_player_id(payload, allow_generic_id=True)
    return Response(json.dumps(shared.get_relationships(shared.safe_int(player_id, 0))), mimetype="application/json")


@app.route("/api/relationships/v1/<action>", methods=["GET"])
@app.route("/api/relationships/v1/<action>/", methods=["GET"])
def relationships_action(action: str) -> Any:
    id1 = shared.safe_int(request.args.get("id1", 0), 0)
    id2 = shared.safe_int(request.args.get("id2", 0), 0)
    return Response(json.dumps(shared.apply_relationship_action(action, id1, id2)), mimetype="application/json")


@app.route("/api/playerReputation/v1/heal", methods=["POST"])
@app.route("/api/playerReputation/v1/heal/", methods=["POST"])
def player_reputation_heal_v1() -> Any:
    payload = request_payload()
    player_id = resolve_local_player_id(payload, allow_generic_id=True)
    remember_local_player_id(player_id)
    good_karma_minutes = shared.safe_int(payload.get("GoodKarmaMinutes", payload.get("goodKarmaMinutes", 0)), 0)
    shared.record_good_karma(player_id, good_karma_minutes)
    return jsonify({"ok": True, "PlayerId": player_id, "GoodKarmaMinutes": good_karma_minutes})


@app.route("/api/players/v1/score", methods=["POST"])
@app.route("/api/players/v1/score/", methods=["POST"])
def players_score_v1() -> Any:
    payload = request_payload()
    player_id = resolve_local_player_id(payload, allow_generic_id=True)
    remember_local_player_id(player_id)
    try:
        score_value = float(payload.get("Score", payload.get("score", 0)) or 0)
    except Exception:
        score_value = 0.0
    secondary_raw = payload.get("SecondaryScore", payload.get("secondaryScore", None))
    try:
        secondary_score = None if secondary_raw in (None, "") else float(secondary_raw)
    except Exception:
        secondary_score = None
    shared.record_player_score(
        player_id=player_id,
        session_id=str(payload.get("SessionId", payload.get("sessionId", "")) or ""),
        activity=str(payload.get("Activity", payload.get("activity", "")) or ""),
        category=str(payload.get("Category", payload.get("category", "")) or ""),
        score=score_value,
        comment=str(payload.get("Comment", payload.get("comment", "")) or ""),
        secondary_score=secondary_score,
    )
    return jsonify({"ok": True})


@app.route("/api/PlayerReporting/v1/create", methods=["POST"])
@app.route("/api/PlayerReporting/v1/create/", methods=["POST"])
@app.route("/api/playerreporting/v1/create", methods=["POST"])
@app.route("/api/playerreporting/v1/create/", methods=["POST"])
def player_reporting_create_v1() -> Any:
    payload = request_payload()
    reporter_player_id = resolve_local_player_id(payload, allow_generic_id=True)
    remember_local_player_id(reporter_player_id)
    shared.record_player_report(
        reporter_player_id=reporter_player_id,
        reported_player_id=shared.safe_int(payload.get("PlayerIdReported", payload.get("playerIdReported", 0)), 0),
        report_category=shared.safe_int(payload.get("ReportCategory", payload.get("reportCategory", 0)), 0),
        activity=str(payload.get("Activity", payload.get("activity", "")) or ""),
    )
    return jsonify({"ok": True})


@app.route("/api/analytics/v1/session/event", methods=["POST"])
@app.route("/api/analytics/v1/session/event/", methods=["POST"])
def analytics_session_event() -> Any:
    payload = request_payload()
    try:
        notifier.emit_analytics_event(payload)
    except Exception:
        pass
    return jsonify({"ok": True})


@app.route("/api/notification/v2", methods=["GET", "POST"])
@app.route("/api/notification/v2/", methods=["GET", "POST"])
def notifications_http_placeholder_v2() -> Any:
    return jsonify({"ok": True, "transport": "websocket", "path": "/api/notification/v2"})


@app.route("/api/notification/v1", methods=["GET", "POST"])
@app.route("/api/notification/v1/", methods=["GET", "POST"])
def notifications_http_placeholder() -> Any:
    return jsonify({"ok": True, "transport": "websocket", "path": "/api/notification/v1"})


@app.route("/api/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def api_fallback(subpath: str) -> Any:
    full_path = f"/api/{subpath}"
    response = jsonify({"ok": True, "path": full_path, "method": request.method, "note": "Fallback response"})
    response.headers['X-RR-Log-Note'] = 'api-fallback'
    return response


@app.errorhandler(404)
def not_found_error(_: Any) -> Any:
    response = jsonify({'error': 'not found'})
    response.status_code = 404
    return response


if __name__ == "__main__":
    ensure_runtime_ready()
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
