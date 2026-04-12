from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

app = Flask(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", ".")).resolve()
PLAYERS_PATH = DATA_DIR / "players.json"
REQUESTS_PATH = DATA_DIR / "request_log.json"

DEFAULT_PLAYER_NAME = os.environ.get("DEFAULT_PLAYER_NAME", "Eduard")
AUTO_CREATE_ON_GET = os.environ.get("AUTO_CREATE_ON_GET", "true").strip().lower() in {"1", "true", "yes", "y"}
DEFAULT_PLATFORM = int(os.environ.get("DEFAULT_PLATFORM", "0"))
DEFAULT_REPUTATION = int(os.environ.get("DEFAULT_REPUTATION", "0"))
DEFAULT_LEVEL = int(os.environ.get("DEFAULT_LEVEL", "1"))
DEFAULT_XP = int(os.environ.get("DEFAULT_XP", "0"))

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

def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return deepcopy(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return deepcopy(default)

def save_json(path: Path, payload: Any) -> None:
    ensure_data_dir()
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

def load_players() -> dict[str, dict[str, Any]]:
    payload = load_json(PLAYERS_PATH, {})
    return payload if isinstance(payload, dict) else {}

def save_players(players: dict[str, dict[str, Any]]) -> None:
    save_json(PLAYERS_PATH, players)

def load_requests() -> list[dict[str, Any]]:
    payload = load_json(REQUESTS_PATH, [])
    return payload if isinstance(payload, list) else []

def save_requests(rows: list[dict[str, Any]]) -> None:
    save_json(REQUESTS_PATH, rows[-500:])

def log_request() -> None:
    rows = load_requests()
    try:
        body = request.get_json(silent=True)
    except Exception:
        body = None
    entry = {
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "method": request.method,
        "path": request.path,
        "query": request.args.to_dict(flat=False),
        "headers": {k: v for k, v in request.headers.items()},
        "json": body,
        "raw_body": request.get_data(cache=True, as_text=True),
    }
    rows.append(entry)
    save_requests(rows)

def make_player(platform: int, platform_id: int | str, name: str | None = None) -> dict[str, Any]:
    platform_id_int = int(platform_id)
    return {
        "Id": abs(hash(f"{platform}:{platform_id_int}")) % 2147483647 or 1,
        "Platform": int(platform),
        "PlatformId": platform_id_int,
        "Name": name or DEFAULT_PLAYER_NAME,
        "XP": DEFAULT_XP,
        "Level": max(1, DEFAULT_LEVEL),
        "Reputation": DEFAULT_REPUTATION,
    }

def parse_platform(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw.isdigit():
            return int(raw)
        if raw in {"steam"}:
            return 0
        if raw in {"oculus"}:
            return 1
    return DEFAULT_PLATFORM

def normalize_player_payload(payload: Any, fallback_platform: int = DEFAULT_PLATFORM, fallback_platform_id: int = 0) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return make_player(fallback_platform, fallback_platform_id)

    platform = parse_platform(payload.get("Platform", fallback_platform))
    platform_id = payload.get("PlatformId", fallback_platform_id)
    if isinstance(platform_id, str) and platform_id.isdigit():
        platform_id = int(platform_id)
    elif not isinstance(platform_id, int):
        platform_id = fallback_platform_id

    player = make_player(platform, platform_id, str(payload.get("Name") or DEFAULT_PLAYER_NAME))
    for key in ("Id", "XP", "Level", "Reputation"):
        if key in payload:
            try:
                player[key] = int(payload[key])
            except Exception:
                pass
    if player["Level"] < 1:
        player["Level"] = 1
    if player["XP"] < 0:
        player["XP"] = 0
    if player["Name"] is None:
        player["Name"] = DEFAULT_PLAYER_NAME
    return player

def player_key(platform: int, platform_id: int) -> str:
    return f"{platform}:{platform_id}"

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

@app.before_request
def before_request() -> None:
    log_request()

@app.get("/health")
def health() -> Any:
    return jsonify({"ok": True, "service": "recroom-2016early-mock"})

@app.get("/__debug/requests")
def debug_requests() -> Any:
    return jsonify(load_requests())

@app.get("/__debug/players")
def debug_players() -> Any:
    return jsonify(load_players())

@app.get("/")
def root() -> Any:
    return jsonify({
        "ok": True,
        "service": "recroom-2016early-mock",
        "hint": "This service is meant to stand in for old Rec Room web endpoints."
    })

@app.route("/api/players/v1/", methods=["GET", "POST", "PUT", "PATCH"])
@app.route("/api/players/v1", methods=["GET", "POST", "PUT", "PATCH"])
@app.route("/api/players/v1/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH"])
def players_v1(subpath: str = "") -> Any:
    players = load_players()
    raw_body = request.get_json(silent=True) or {}

    pieces = [p for p in subpath.split("/") if p]
    platform = DEFAULT_PLATFORM
    platform_id = 0

    if len(pieces) >= 2 and pieces[0].isdigit() and pieces[1].isdigit():
        platform = int(pieces[0])
        platform_id = int(pieces[1])
    elif len(pieces) >= 1 and pieces[0].isdigit():
        for existing in players.values():
            if int(existing.get("Id", -1)) == int(pieces[0]):
                return jsonify(existing)
        platform_id = int(pieces[0])
    else:
        body_platform = raw_body.get("Platform", DEFAULT_PLATFORM)
        body_platform_id = raw_body.get("PlatformId", 0)
        platform = parse_platform(body_platform)
        try:
            platform_id = int(body_platform_id)
        except Exception:
            platform_id = 0

    key = player_key(platform, platform_id)

    if request.method == "GET":
        player = get_or_create_player(platform, platform_id)
        if player is None:
            return jsonify({"error": "player not found"}), 404
        return jsonify(player)

    if request.method == "POST":
        player = normalize_player_payload(raw_body, platform, platform_id)
        key = player_key(int(player["Platform"]), int(player["PlatformId"]))
        players[key] = player
        save_players(players)
        return jsonify(player), 201

    if key not in players:
        players[key] = make_player(platform, platform_id)

    current = players[key]
    incoming = normalize_player_payload(raw_body, int(current["Platform"]), int(current["PlatformId"]))
    current.update({
        "Name": incoming["Name"],
        "XP": incoming["XP"],
        "Level": incoming["Level"],
        "Reputation": incoming["Reputation"],
        "Platform": incoming["Platform"],
        "PlatformId": incoming["PlatformId"],
    })
    if "Id" in raw_body:
        try:
            current["Id"] = int(raw_body["Id"])
        except Exception:
            pass
    players[player_key(int(current["Platform"]), int(current["PlatformId"]))] = current
    if key != player_key(int(current["Platform"]), int(current["PlatformId"])) and key in players:
        del players[key]
    save_players(players)
    return jsonify(current)

@app.route("/api/objectives/v1/", methods=["GET"])
@app.route("/api/objectives/v1", methods=["GET"])
@app.route("/api/objectives/v1/<path:subpath>", methods=["GET"])
@app.route("/api/dailyobjectives/v1/", methods=["GET"])
@app.route("/api/dailyobjectives/v1", methods=["GET"])
@app.route("/api/dailyobjectives/v1/<path:subpath>", methods=["GET"])
def objectives_v1(subpath: str = "") -> Any:
    return jsonify({
        "DateUtc": datetime.now(timezone.utc).date().isoformat(),
        "DailyObjectives": DEFAULT_OBJECTIVES,
    })

@app.route("/api/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def api_fallback(subpath: str) -> Any:
    body = request.get_json(silent=True)
    return jsonify({
        "ok": True,
        "path": f"/api/{subpath}",
        "method": request.method,
        "note": "Fallback response. Check /__debug/requests to see what the game asked for.",
        "echo": body,
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
