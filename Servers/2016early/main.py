from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import random
import threading
import time
from collections import defaultdict, deque
from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, request

app = Flask(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", ".")).resolve()
PLAYERS_PATH = DATA_DIR / "players.json"
REQUESTS_PATH = DATA_DIR / "request_log.json"
OBJECTIVES_CONFIG_V1_PATH = DATA_DIR / "objectives_config_v1.json"
MOTD_PATH = DATA_DIR / "motd.txt"
IMAGES_DIR = DATA_DIR / "player_images"
VERIFY_LOG_PATH = DATA_DIR / "verification_requests.json"

DEFAULT_PLAYER_NAME = os.environ.get("DEFAULT_PLAYER_NAME", "Eduard")
AUTO_CREATE_ON_GET = os.environ.get("AUTO_CREATE_ON_GET", "true").strip().lower() in {"1", "true", "yes", "y"}
DEFAULT_PLATFORM = int(os.environ.get("DEFAULT_PLATFORM", "0"))
DEFAULT_REPUTATION = int(os.environ.get("DEFAULT_REPUTATION", "0"))
DEFAULT_LEVEL = int(os.environ.get("DEFAULT_LEVEL", "1"))
DEFAULT_XP = int(os.environ.get("DEFAULT_XP", "0"))
DEFAULT_MOTD_TEXT = os.environ.get("DEFAULT_MOTD_TEXT", "Online on RecNet! Welcome to Rec Room!")
ENABLE_DEBUG_ENDPOINTS = os.environ.get("ENABLE_DEBUG_ENDPOINTS", "false").strip().lower() in {"1", "true", "yes", "y"}
TRUST_PROXY_HEADERS = os.environ.get("TRUST_PROXY_HEADERS", "true").strip().lower() in {"1", "true", "yes", "y"}
REQUEST_LOG_RETENTION = max(10, int(os.environ.get("REQUEST_LOG_RETENTION", "500")))
AUTO_VERIFY_EMAIL = os.environ.get("AUTO_VERIFY_EMAIL", "false").strip().lower() in {"1", "true", "yes", "y"}
REQUIRE_AUTH = os.environ.get("REQUIRE_AUTH", "false").strip().lower() in {"1", "true", "yes", "y"}
AUTH_USERNAME = os.environ.get("AUTH_USERNAME", "recroom@gmail.com")
AUTH_PASSWORD = os.environ.get("AUTH_PASSWORD", "recnet87")
LOG_SALT = os.environ.get("LOG_SALT", "rec-room-local-salt")
MAX_REQUEST_BODY_BYTES = max(1024, int(os.environ.get("MAX_REQUEST_BODY_BYTES", str(4 * 1024 * 1024))))
GENERAL_RATE_LIMIT = max(10, int(os.environ.get("GENERAL_RATE_LIMIT", "180")))
GENERAL_RATE_WINDOW_SECONDS = max(1, int(os.environ.get("GENERAL_RATE_WINDOW_SECONDS", "60")))
MUTATION_RATE_LIMIT = max(5, int(os.environ.get("MUTATION_RATE_LIMIT", "60")))
MUTATION_RATE_WINDOW_SECONDS = max(1, int(os.environ.get("MUTATION_RATE_WINDOW_SECONDS", "60")))
IMAGE_RATE_LIMIT = max(2, int(os.environ.get("IMAGE_RATE_LIMIT", "20")))
IMAGE_RATE_WINDOW_SECONDS = max(1, int(os.environ.get("IMAGE_RATE_WINDOW_SECONDS", "60")))
AUTH_FAILURE_RATE_LIMIT = max(2, int(os.environ.get("AUTH_FAILURE_RATE_LIMIT", "20")))
AUTH_FAILURE_WINDOW_SECONDS = max(1, int(os.environ.get("AUTH_FAILURE_WINDOW_SECONDS", "60")))

app.config["MAX_CONTENT_LENGTH"] = MAX_REQUEST_BODY_BYTES

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

DEFAULT_OBJECTIVE_POOL = [
    {"type": 100, "score": 1, "xp": 100},
    {"type": 101, "score": 1, "xp": 100},
    {"type": 200, "score": 1, "xp": 100},
    {"type": 201, "score": 1, "xp": 100},
    {"type": 202, "score": 3, "xp": 100},
    {"type": 300, "score": 1, "xp": 100},
    {"type": 301, "score": 1, "xp": 100},
    {"type": 302, "score": 5, "xp": 100},
    {"type": 400, "score": 1, "xp": 100},
    {"type": 402, "score": 5, "xp": 100},
    {"type": 500, "score": 1, "xp": 100},
    {"type": 501, "score": 1, "xp": 100},
    {"type": 502, "score": 5, "xp": 100},
    {"type": 603, "score": 1, "xp": 100},
    {"type": 701, "score": 1, "xp": 100},
    {"type": 702, "score": 5, "xp": 100},
    {"type": 801, "score": 1, "xp": 100},
    {"type": 802, "score": 2, "xp": 100},
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
        {"type": 702, "score": 5, "xp": 100},
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

_TRANSPARENT_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9s2w0v8AAAAASUVORK5CYII="
)

_rate_limit_lock = threading.Lock()
_rate_limit_buckets: dict[str, deque[float]] = defaultdict(deque)


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return deepcopy(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return deepcopy(default)



def save_json(path: Path, payload: Any) -> None:
    ensure_data_dir()
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    temp_path.replace(path)



def load_players() -> dict[str, dict[str, Any]]:
    payload = load_json(PLAYERS_PATH, {})
    return payload if isinstance(payload, dict) else {}



def save_players(players: dict[str, dict[str, Any]]) -> None:
    save_json(PLAYERS_PATH, players)



def load_requests() -> list[dict[str, Any]]:
    payload = load_json(REQUESTS_PATH, [])
    return payload if isinstance(payload, list) else []



def save_requests(rows: list[dict[str, Any]]) -> None:
    save_json(REQUESTS_PATH, rows[-REQUEST_LOG_RETENTION:])



def load_verification_requests() -> list[dict[str, Any]]:
    payload = load_json(VERIFY_LOG_PATH, [])
    return payload if isinstance(payload, list) else []



def save_verification_requests(rows: list[dict[str, Any]]) -> None:
    save_json(VERIFY_LOG_PATH, rows[-REQUEST_LOG_RETENTION:])



def _normalize_objective_entry(entry: Any) -> dict[str, int] | None:
    if not isinstance(entry, dict):
        return None

    raw_type = entry.get("type", entry.get("ObjectiveType", entry.get("objectiveType")))
    raw_score = entry.get("score", entry.get("RequiredScore", entry.get("requiredScore")))
    raw_xp = entry.get("xp", entry.get("Xp", entry.get("XP", entry.get("rewardXp", 100))))

    try:
        objective_type = int(raw_type)
        required_score = int(raw_score)
        xp = int(raw_xp)
    except Exception:
        return None

    if objective_type < 0 or required_score <= 0:
        return None

    if objective_type == 102:
        objective_type = 101

    return {"type": objective_type, "score": required_score, "xp": max(1, xp)}



def normalize_objectives_config_v1(payload: Any) -> list[list[dict[str, int]]]:
    normalized_days: list[list[dict[str, int]]] = []

    if isinstance(payload, dict):
        payload = payload.get("DailyObjectives", payload.get("objectives", payload.get("days", payload)))

    if not isinstance(payload, list):
        return deepcopy(DEFAULT_OBJECTIVES_CONFIG_V1)

    for raw_day in payload:
        day_objectives: list[dict[str, int]] = []

        if isinstance(raw_day, dict):
            raw_day = raw_day.get("Objectives", raw_day.get("objectives", raw_day))

        if not isinstance(raw_day, list):
            continue

        for raw_objective in raw_day:
            normalized = _normalize_objective_entry(raw_objective)
            if normalized is not None:
                day_objectives.append(normalized)

        if day_objectives:
            normalized_days.append(day_objectives[:3])

    if not normalized_days:
        return deepcopy(DEFAULT_OBJECTIVES_CONFIG_V1)

    while len(normalized_days) < 7:
        normalized_days.append(deepcopy(normalized_days[len(normalized_days) % len(normalized_days)]))

    return normalized_days[:7]



def _normalize_objective_pool(payload: Any) -> list[dict[str, int]]:
    if isinstance(payload, dict):
        payload = payload.get("pool", payload.get("objectives", payload.get("entries", payload)))

    if not isinstance(payload, list):
        return deepcopy(DEFAULT_OBJECTIVE_POOL)

    normalized_pool: list[dict[str, int]] = []
    seen_types: set[int] = set()

    for raw_objective in payload:
        normalized = _normalize_objective_entry(raw_objective)
        if normalized is None:
            continue

        objective_type = int(normalized["type"])
        if objective_type in seen_types:
            continue

        seen_types.add(objective_type)
        normalized_pool.append(normalized)

    return normalized_pool if len(normalized_pool) >= 3 else deepcopy(DEFAULT_OBJECTIVE_POOL)



def _current_week_seed(today: date | None = None) -> int:
    today = today or datetime.now(timezone.utc).date()
    iso_year, iso_week, _ = today.isocalendar()
    return iso_year * 100 + iso_week



def generate_weekly_objectives_config_v1(today: date | None = None, pool: Any = None) -> list[list[dict[str, int]]]:
    normalized_pool = _normalize_objective_pool(pool if pool is not None else DEFAULT_OBJECTIVE_POOL)
    rng = random.Random(_current_week_seed(today))

    if len(normalized_pool) < 3:
        return deepcopy(DEFAULT_OBJECTIVES_CONFIG_V1)

    working_pool = [deepcopy(item) for item in normalized_pool]
    rng.shuffle(working_pool)

    weekly_days: list[list[dict[str, int]]] = []
    cursor = 0

    for _ in range(7):
        day: list[dict[str, int]] = []
        seen_types: set[int] = set()

        while len(day) < 3:
            if cursor >= len(working_pool):
                working_pool = [deepcopy(item) for item in normalized_pool]
                rng.shuffle(working_pool)
                cursor = 0

            candidate = deepcopy(working_pool[cursor])
            cursor += 1

            objective_type = int(candidate["type"])
            if objective_type in seen_types:
                continue

            seen_types.add(objective_type)
            day.append(candidate)

        weekly_days.append(day)

    return weekly_days



def load_objectives_config_v1(today: date | None = None) -> list[list[dict[str, int]]]:
    if not OBJECTIVES_CONFIG_V1_PATH.exists():
        return generate_weekly_objectives_config_v1(today=today)

    payload = load_json(OBJECTIVES_CONFIG_V1_PATH, None)

    if isinstance(payload, dict):
        mode = str(payload.get("mode", "")).strip().lower()

        if mode == "weekly":
            pool = payload.get("pool", DEFAULT_OBJECTIVE_POOL)
            return generate_weekly_objectives_config_v1(today=today, pool=pool)

        if "pool" in payload and "days" not in payload and "DailyObjectives" not in payload and "objectives" not in payload:
            return generate_weekly_objectives_config_v1(today=today, pool=payload.get("pool"))

    if payload is None:
        return generate_weekly_objectives_config_v1(today=today)

    return normalize_objectives_config_v1(payload)



def load_motd_text() -> str:
    if MOTD_PATH.exists():
        try:
            return MOTD_PATH.read_text(encoding="utf-8")
        except Exception:
            pass
    return DEFAULT_MOTD_TEXT



def save_motd_text(text: str) -> None:
    ensure_data_dir()
    MOTD_PATH.write_text(text, encoding="utf-8")



def normalize_motd_payload(payload: Any, raw_text: str) -> str:
    if isinstance(payload, dict):
        for key in ("motd", "message", "text", "value", "content"):
            value = payload.get(key)
            if value is not None:
                return str(value)
    stripped = raw_text.strip("﻿")
    if stripped:
        return stripped
    return DEFAULT_MOTD_TEXT



def parse_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}



def parse_platform(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw.lstrip("-").isdigit():
            return int(raw)
        if raw in {"steam"}:
            return 0
        if raw in {"oculus"}:
            return 1
    return DEFAULT_PLATFORM



def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return default
        return int(value)
    except Exception:
        return default



def _stable_player_id(platform: int, platform_id: int) -> int:
    digest = hashlib.sha256(f"{platform}:{platform_id}".encode("utf-8")).digest()
    raw_value = int.from_bytes(digest[:8], "big") & 0x7FFFFFFF
    return raw_value or 1



def make_player(platform: int, platform_id: int | str, name: str | None = None) -> dict[str, Any]:
    platform_id_int = int(platform_id)
    return {
        "Id": _stable_player_id(platform, platform_id_int),
        "Platform": int(platform),
        "PlatformId": platform_id_int,
        "Name": name or DEFAULT_PLAYER_NAME,
        "XP": max(0, DEFAULT_XP),
        "Level": max(1, DEFAULT_LEVEL),
        "Reputation": DEFAULT_REPUTATION,
        "Email": "",
        "Username": "",
        "Verified": False,
    }



def normalize_player_payload(payload: Any, fallback_platform: int = DEFAULT_PLATFORM, fallback_platform_id: int = 0) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return make_player(fallback_platform, fallback_platform_id)

    platform = parse_platform(payload.get("Platform", fallback_platform))
    platform_id = payload.get("PlatformId", fallback_platform_id)
    if isinstance(platform_id, str) and platform_id.strip().isdigit():
        platform_id = int(platform_id.strip())
    elif not isinstance(platform_id, int):
        platform_id = fallback_platform_id

    player = make_player(platform, platform_id, str(payload.get("Name") or DEFAULT_PLAYER_NAME))

    for key in ("Id", "XP", "Level", "Reputation"):
        if key in payload:
            player[key] = _safe_int(payload.get(key), player[key])

    player["Email"] = str(payload.get("Email") or "")
    player["Username"] = str(payload.get("Username") or "")
    player["Verified"] = parse_bool(payload.get("Verified"), False)

    if player["Level"] < 1:
        player["Level"] = 1
    if player["XP"] < 0:
        player["XP"] = 0
    if not player["Name"]:
        player["Name"] = DEFAULT_PLAYER_NAME
    if player["Id"] <= 0:
        player["Id"] = _stable_player_id(player["Platform"], player["PlatformId"])
    return player



def player_key(platform: int, platform_id: int) -> str:
    return f"{platform}:{platform_id}"



def get_player_by_id(player_id: int) -> dict[str, Any] | None:
    for player in load_players().values():
        if _safe_int(player.get("Id"), -1) == int(player_id):
            return player
    return None



def get_or_create_player(platform: int, platform_id: int) -> dict[str, Any] | None:
    players = load_players()
    key = player_key(platform, platform_id)
    if key in players:
        return players[key]
    if not AUTO_CREATE_ON_GET:
        return None
    player = make_player(platform, platform_id)
    players[key] = player
    save_players(players)
    return player



def _redact_value(value: Any, key_hint: str = "") -> Any:
    hint = key_hint.lower()
    if isinstance(value, dict):
        return {str(k): _redact_value(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_value(item, key_hint) for item in value[:25]]
    if value is None:
        return None
    text = str(value)
    if hint in {"authorization", "cookie", "set-cookie", "password", "image", "raw_body"}:
        return "<redacted>"
    if "email" in hint and text:
        return "<redacted-email>"
    if len(text) > 256:
        return text[:256] + "…"
    return text



def _hash_for_log(text: str) -> str:
    return hashlib.sha256(f"{LOG_SALT}:{text}".encode("utf-8")).hexdigest()[:16]



def get_client_ip() -> str:
    if TRUST_PROXY_HEADERS:
        forwarded_for = request.headers.get("X-Forwarded-For", "")
        if forwarded_for:
            return forwarded_for.split(",", 1)[0].strip() or (request.remote_addr or "unknown")
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()
    return request.remote_addr or "unknown"



def _get_rate_limit_bucket_name() -> tuple[str, int, int]:
    path = request.path
    if path.startswith("/api/images/v1/") and request.method in {"POST", "PUT", "PATCH"}:
        return ("image_mutation", IMAGE_RATE_LIMIT, IMAGE_RATE_WINDOW_SECONDS)
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        return ("mutation", MUTATION_RATE_LIMIT, MUTATION_RATE_WINDOW_SECONDS)
    return ("general", GENERAL_RATE_LIMIT, GENERAL_RATE_WINDOW_SECONDS)



def _check_rate_limit(bucket_name: str, limit: int, window_seconds: int) -> bool:
    client_key = f"{bucket_name}:{get_client_ip()}"
    now = time.time()
    with _rate_limit_lock:
        bucket = _rate_limit_buckets[client_key]
        cutoff = now - window_seconds
        while bucket and bucket[0] <= cutoff:
            bucket.popleft()
        if len(bucket) >= limit:
            return False
        bucket.append(now)
    return True



def _record_auth_failure() -> bool:
    return _check_rate_limit("auth_fail", AUTH_FAILURE_RATE_LIMIT, AUTH_FAILURE_WINDOW_SECONDS)



def _constant_time_auth_compare(provided: str, expected: str) -> bool:
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))



def _is_authenticated() -> bool:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header:
        return not REQUIRE_AUTH
    if not auth_header.startswith("Basic "):
        return False
    try:
        raw_value = base64.b64decode(auth_header[6:].strip()).decode("utf-8")
    except Exception:
        return False
    username, separator, password = raw_value.partition(":")
    if not separator:
        return False
    return _constant_time_auth_compare(username, AUTH_USERNAME) and _constant_time_auth_compare(password, AUTH_PASSWORD)



def _extract_request_payload() -> dict[str, Any]:
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



def _find_player_from_request_context(players: dict[str, dict[str, Any]], subpath: str = "") -> tuple[int, int, dict[str, Any] | None]:
    payload = _extract_request_payload()
    pieces = [p for p in subpath.split("/") if p]

    if len(pieces) == 1 and pieces[0].isdigit():
        existing = get_player_by_id(int(pieces[0]))
        if existing is not None:
            return _safe_int(existing.get("Platform"), DEFAULT_PLATFORM), _safe_int(existing.get("PlatformId"), 0), existing

    if len(pieces) >= 2 and pieces[0].lstrip("-").isdigit() and pieces[1].isdigit():
        platform = int(pieces[0])
        platform_id = int(pieces[1])
        return platform, platform_id, players.get(player_key(platform, platform_id))

    query_platform = parse_platform(payload.get("p", payload.get("platform", payload.get("Platform", DEFAULT_PLATFORM))))
    query_platform_id = _safe_int(payload.get("id", payload.get("platformId", payload.get("PlatformId", 0))), 0)
    existing = players.get(player_key(query_platform, query_platform_id))

    if existing is not None:
        return query_platform, query_platform_id, existing

    if len(pieces) == 1 and pieces[0].isdigit():
        return query_platform, int(pieces[0]), get_player_by_id(int(pieces[0]))

    return query_platform, query_platform_id, existing



def log_request() -> None:
    rows = load_requests()
    payload = _extract_request_payload()
    entry = {
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "method": request.method,
        "path": request.path,
        "query": {str(k): _redact_value(v, str(k)) for k, v in request.args.to_dict(flat=False).items()},
        "content_type": request.content_type,
        "content_length": request.content_length or 0,
        "client_hash": _hash_for_log(get_client_ip()),
        "auth_present": bool(request.headers.get("Authorization")),
        "json_or_form": _redact_value(payload),
    }
    rows.append(entry)
    save_requests(rows)



def _debug_enabled_response() -> Any:
    return jsonify({"error": "not found"}), 404



def _image_path_for_player(player_id: int) -> Path:
    return IMAGES_DIR / f"{player_id}.bin"



def _image_meta_path_for_player(player_id: int) -> Path:
    return IMAGES_DIR / f"{player_id}.json"



def _load_player_image(player_id: int) -> tuple[bytes, str]:
    image_path = _image_path_for_player(player_id)
    meta_path = _image_meta_path_for_player(player_id)
    if image_path.exists():
        try:
            payload = image_path.read_bytes()
            meta = load_json(meta_path, {})
            content_type = str(meta.get("content_type") or "image/png") if isinstance(meta, dict) else "image/png"
            return payload, content_type
        except Exception:
            pass
    return _TRANSPARENT_PNG, "image/png"



def _save_player_image(player_id: int, image_bytes: bytes, content_type: str) -> None:
    ensure_data_dir()
    _image_path_for_player(player_id).write_bytes(image_bytes)
    save_json(_image_meta_path_for_player(player_id), {"content_type": content_type or "application/octet-stream"})



def _sanitize_player_for_response(player: dict[str, Any]) -> dict[str, Any]:
    sanitized = {
        "Id": _safe_int(player.get("Id"), 0),
        "Platform": _safe_int(player.get("Platform"), DEFAULT_PLATFORM),
        "PlatformId": _safe_int(player.get("PlatformId"), 0),
        "Name": str(player.get("Name") or DEFAULT_PLAYER_NAME),
        "XP": max(0, _safe_int(player.get("XP"), 0)),
        "Level": max(1, _safe_int(player.get("Level"), 1)),
        "Reputation": _safe_int(player.get("Reputation"), DEFAULT_REPUTATION),
        "Email": str(player.get("Email") or ""),
        "Username": str(player.get("Username") or ""),
        "Verified": parse_bool(player.get("Verified"), False),
    }
    if sanitized["Id"] <= 0:
        sanitized["Id"] = _stable_player_id(sanitized["Platform"], sanitized["PlatformId"])
    return sanitized



@app.before_request
def before_request() -> Any:
    if request.content_length is not None and request.content_length > MAX_REQUEST_BODY_BYTES:
        return jsonify({"error": "payload too large"}), 413

    bucket_name, limit, window_seconds = _get_rate_limit_bucket_name()
    if not _check_rate_limit(bucket_name, limit, window_seconds):
        return jsonify({"error": "rate limit exceeded"}), 429

    if request.path.startswith("/api/") and not _is_authenticated():
        _record_auth_failure()
        return Response("Unauthorized", 401, {"WWW-Authenticate": 'Basic realm="Rec Room API"'})

    log_request()
    return None



@app.after_request
def add_security_headers(response: Response) -> Response:
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Cache-Control"] = response.headers.get("Cache-Control", "no-store")
    return response



@app.get("/health")
def health() -> Any:
    return jsonify({"ok": True, "service": "recroom-2016early-mock"})



@app.get("/__debug/requests")
def debug_requests() -> Any:
    if not ENABLE_DEBUG_ENDPOINTS:
        return _debug_enabled_response()
    return jsonify(load_requests())



@app.get("/__debug/players")
def debug_players() -> Any:
    if not ENABLE_DEBUG_ENDPOINTS:
        return _debug_enabled_response()
    return jsonify(load_players())



@app.get("/")
def root() -> Any:
    return jsonify(
        {
            "ok": True,
            "service": "recroom-2016early-mock",
            "hint": "This service is meant to stand in for old Rec Room web endpoints.",
        }
    )



@app.route("/api/players/v1/create", methods=["POST"])
def players_v1_create() -> Any:
    players = load_players()
    payload = _extract_request_payload()
    platform = parse_platform(payload.get("Platform", payload.get("platform", DEFAULT_PLATFORM)))
    platform_id = _safe_int(payload.get("PlatformId", payload.get("platformId", payload.get("id", 0))), 0)
    player = normalize_player_payload(payload, platform, platform_id)
    key = player_key(int(player["Platform"]), int(player["PlatformId"]))

    existing = players.get(key)
    if existing is not None:
        merged = _sanitize_player_for_response({**existing, **player})
        players[key] = merged
        save_players(players)
        return jsonify(merged)

    players[key] = _sanitize_player_for_response(player)
    save_players(players)
    return jsonify(players[key]), 201



@app.route("/api/players/v1/update/<int:player_id>", methods=["POST", "PUT", "PATCH"])
def players_v1_update(player_id: int) -> Any:
    players = load_players()
    payload = _extract_request_payload()
    current = get_player_by_id(player_id)
    if current is None:
        platform = parse_platform(payload.get("Platform", payload.get("platform", DEFAULT_PLATFORM)))
        platform_id = _safe_int(payload.get("PlatformId", payload.get("platformId", 0)), 0)
        current = make_player(platform, platform_id)

    fallback_platform = _safe_int(current.get("Platform"), DEFAULT_PLATFORM)
    fallback_platform_id = _safe_int(current.get("PlatformId"), 0)
    incoming = normalize_player_payload(payload, fallback_platform, fallback_platform_id)
    merged = _sanitize_player_for_response({**current, **incoming, "Id": player_id})

    old_key = player_key(fallback_platform, fallback_platform_id)
    new_key = player_key(merged["Platform"], merged["PlatformId"])

    if old_key in players:
        del players[old_key]
    players[new_key] = merged
    save_players(players)
    return jsonify(merged)



@app.route("/api/players/v1/verify/<int:player_id>", methods=["POST"])
def players_v1_verify(player_id: int) -> Any:
    players = load_players()
    current = get_player_by_id(player_id)
    if current is None:
        return jsonify({"Message": "Player not found."}), 404

    payload = _extract_request_payload()
    submitted_email = str(payload.get("email") or payload.get("Email") or "").strip()
    if not submitted_email:
        return jsonify({"Message": "Email is required."}), 400

    current["Email"] = submitted_email
    if AUTO_VERIFY_EMAIL:
        current["Verified"] = True

    current = _sanitize_player_for_response(current)
    players[player_key(current["Platform"], current["PlatformId"])] = current
    save_players(players)

    verification_rows = load_verification_requests()
    verification_rows.append(
        {
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "player_id": current["Id"],
            "email_hash": _hash_for_log(submitted_email),
            "auto_verified": AUTO_VERIFY_EMAIL,
        }
    )
    save_verification_requests(verification_rows)

    message = "Verification queued." if not AUTO_VERIFY_EMAIL else "Verification complete."
    return jsonify({"Message": message})



@app.route("/api/players/v1/", methods=["GET", "POST", "PUT", "PATCH"])
@app.route("/api/players/v1", methods=["GET", "POST", "PUT", "PATCH"])
@app.route("/api/players/v1/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH"])
def players_v1(subpath: str = "") -> Any:
    players = load_players()
    payload = _extract_request_payload()
    platform, platform_id, existing = _find_player_from_request_context(players, subpath)

    if request.method == "GET":
        if existing is not None:
            return jsonify(_sanitize_player_for_response(existing))
        player = get_or_create_player(platform, platform_id)
        if player is None:
            return jsonify({"error": "player not found"}), 404
        return jsonify(_sanitize_player_for_response(player))

    if request.method == "POST":
        player = normalize_player_payload(payload, platform, platform_id)
        key = player_key(int(player["Platform"]), int(player["PlatformId"]))
        merged = _sanitize_player_for_response({**(players.get(key) or {}), **player})
        players[key] = merged
        save_players(players)
        return jsonify(merged), 201

    current = existing if existing is not None else make_player(platform, platform_id)
    incoming = normalize_player_payload(payload, _safe_int(current.get("Platform"), platform), _safe_int(current.get("PlatformId"), platform_id))
    merged = _sanitize_player_for_response({**current, **incoming})
    old_key = player_key(_safe_int(current.get("Platform"), platform), _safe_int(current.get("PlatformId"), platform_id))
    new_key = player_key(merged["Platform"], merged["PlatformId"])
    if old_key in players and old_key != new_key:
        del players[old_key]
    players[new_key] = merged
    save_players(players)
    return jsonify(merged)



@app.route("/api/images/v1/profile/<int:player_id>", methods=["GET", "POST", "PUT"])
def player_profile_image(player_id: int) -> Any:
    player = get_player_by_id(player_id)
    if player is None:
        return jsonify({"error": "player not found"}), 404

    if request.method == "GET":
        image_bytes, content_type = _load_player_image(player_id)
        return Response(image_bytes, mimetype=content_type)

    image_bytes = b""
    content_type = "application/octet-stream"

    if "image" in request.files:
        upload = request.files["image"]
        image_bytes = upload.read()
        content_type = upload.mimetype or content_type
    else:
        image_bytes = request.get_data(cache=True)
        content_type = request.content_type or content_type

    if not image_bytes:
        return jsonify({"error": "image payload missing"}), 400
    if len(image_bytes) > MAX_REQUEST_BODY_BYTES:
        return jsonify({"error": "payload too large"}), 413

    _save_player_image(player_id, image_bytes, content_type)
    return jsonify({"ok": True, "playerId": player_id, "bytes": len(image_bytes)})



@app.route("/api/objectives/v1/", methods=["GET"])
@app.route("/api/objectives/v1", methods=["GET"])
@app.route("/api/objectives/v1/<path:subpath>", methods=["GET"])
@app.route("/api/dailyobjectives/v1/", methods=["GET"])
@app.route("/api/dailyobjectives/v1", methods=["GET"])
@app.route("/api/dailyobjectives/v1/<path:subpath>", methods=["GET"])
def objectives_v1(subpath: str = "") -> Any:
    return jsonify({"DateUtc": datetime.now(timezone.utc).date().isoformat(), "DailyObjectives": DEFAULT_OBJECTIVES})



@app.route("/api/config/v1/objectives", methods=["GET"])
@app.route("/api/config/v1/objectives/", methods=["GET"])
def objectives_config_v1() -> Any:
    return jsonify(load_objectives_config_v1())



@app.route("/api/config/v1/motd", methods=["GET", "POST", "PUT"])
@app.route("/api/config/v1/motd/", methods=["GET", "POST", "PUT"])
def motd_config_v1() -> Any:
    if request.method == "GET":
        return Response(load_motd_text(), mimetype="text/plain")

    payload = _extract_request_payload()
    raw_text = request.get_data(cache=True, as_text=True)
    text = normalize_motd_payload(payload, raw_text)
    save_motd_text(text)
    return jsonify({"ok": True, "motd": text})



@app.route("/api/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def api_fallback(subpath: str) -> Any:
    return jsonify(
        {
            "ok": True,
            "path": f"/api/{subpath}",
            "method": request.method,
            "note": "Fallback response. Check the server-side request log if you need to implement this route explicitly.",
        }
    )



if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
