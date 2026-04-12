from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

app = Flask(__name__)

STORE_LOCK = threading.Lock()
REQUEST_LOCK = threading.Lock()
REQUEST_LOG_LIMIT = int(os.getenv('REQUEST_LOG_LIMIT', '200'))
PLAYER_STORE_PATH = os.getenv('PLAYER_STORE_PATH', '').strip()
DEFAULT_PLATFORM = int(os.getenv('DEFAULT_PLATFORM', '0'))
DEFAULT_NAME = os.getenv('DEFAULT_PLAYER_NAME', 'RailwayPlayer')
AUTO_CREATE_ON_GET = os.getenv('AUTO_CREATE_ON_GET', 'true').strip().lower() in {'1', 'true', 'yes', 'on'}


@dataclass
class PlayerModel:
    Id: int
    Platform: int
    PlatformId: int
    Name: str
    XP: int
    Level: int
    Reputation: int


players: dict[str, PlayerModel] = {}
request_log: list[dict[str, Any]] = []
next_player_id = 1


def load_store() -> None:
    global next_player_id
    if not PLAYER_STORE_PATH:
        return
    path = Path(PLAYER_STORE_PATH)
    if not path.exists():
        return
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return

    raw_players = data.get('players', {})
    raw_next_id = data.get('next_player_id', 1)
    loaded: dict[str, PlayerModel] = {}

    for key, value in raw_players.items():
        try:
            loaded[str(key)] = PlayerModel(
                Id=int(value.get('Id', 0)),
                Platform=int(value.get('Platform', DEFAULT_PLATFORM)),
                PlatformId=int(value.get('PlatformId', int(key))),
                Name=str(value.get('Name', DEFAULT_NAME)),
                XP=int(value.get('XP', 0)),
                Level=max(1, int(value.get('Level', 1))),
                Reputation=int(value.get('Reputation', 0)),
            )
        except Exception:
            continue

    players.update(loaded)
    next_player_id = max(int(raw_next_id), 1)
    for model in players.values():
        next_player_id = max(next_player_id, model.Id + 1)


def save_store() -> None:
    if not PLAYER_STORE_PATH:
        return
    path = Path(PLAYER_STORE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'next_player_id': next_player_id,
        'players': {key: asdict(value) for key, value in players.items()},
    }
    path.write_text(json.dumps(payload, indent=2), encoding='utf-8')


def add_request_log(entry: dict[str, Any]) -> None:
    with REQUEST_LOCK:
        request_log.append(entry)
        if len(request_log) > REQUEST_LOG_LIMIT:
            del request_log[:-REQUEST_LOG_LIMIT]


def coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def parse_body() -> dict[str, Any]:
    if request.is_json:
        data = request.get_json(silent=True)
        if isinstance(data, dict):
            return data
    form_data = request.form.to_dict(flat=True)
    if form_data:
        return form_data
    raw = request.get_data(cache=True, as_text=True).strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {'raw_body': raw}


def get_steam_id(subpath: str = '') -> str:
    for key in ('steamId', 'steamid', 'platformId', 'playerId', 'id'):
        value = request.args.get(key)
        if value:
            return value.strip()
    body = parse_body()
    for key in ('steamId', 'steamid', 'PlatformId', 'platformId', 'playerId', 'id'):
        value = body.get(key)
        if value not in (None, ''):
            return str(value).strip()
    parts = [part for part in subpath.split('/') if part]
    for part in reversed(parts):
        if part.lower() not in {'steam', 'player', 'players', 'v1'}:
            return part
    return '0'


def build_default_name(steam_id: str) -> str:
    return f'{DEFAULT_NAME}_{steam_id}' if steam_id not in {'', '0'} else DEFAULT_NAME


def get_or_create_player(steam_id: str, body: dict[str, Any] | None = None) -> PlayerModel:
    global next_player_id
    if body is None:
        body = {}

    existing = players.get(steam_id)
    if existing is not None:
        return existing

    name = str(body.get('Name') or body.get('name') or request.args.get('name') or build_default_name(steam_id))
    platform = coerce_int(body.get('Platform') or body.get('platform') or request.args.get('platform'), DEFAULT_PLATFORM)

    model = PlayerModel(
        Id=next_player_id,
        Platform=platform,
        PlatformId=coerce_int(body.get('PlatformId') or body.get('platformId') or steam_id, coerce_int(steam_id, 0)),
        Name=name,
        XP=max(0, coerce_int(body.get('XP') or body.get('xp'), 0)),
        Level=max(1, coerce_int(body.get('Level') or body.get('level'), 1)),
        Reputation=coerce_int(body.get('Reputation') or body.get('reputation'), 0),
    )
    players[steam_id] = model
    next_player_id += 1
    save_store()
    return model


def update_player(model: PlayerModel, body: dict[str, Any]) -> PlayerModel:
    if 'Name' in body or 'name' in body:
        model.Name = str(body.get('Name') or body.get('name') or model.Name)
    if 'Platform' in body or 'platform' in body:
        model.Platform = coerce_int(body.get('Platform') or body.get('platform'), model.Platform)
    if 'PlatformId' in body or 'platformId' in body:
        model.PlatformId = coerce_int(body.get('PlatformId') or body.get('platformId'), model.PlatformId)
    if 'XP' in body or 'xp' in body:
        model.XP = max(0, coerce_int(body.get('XP') or body.get('xp'), model.XP))
    if 'Level' in body or 'level' in body:
        model.Level = max(1, coerce_int(body.get('Level') or body.get('level'), model.Level))
    if 'Reputation' in body or 'reputation' in body:
        model.Reputation = coerce_int(body.get('Reputation') or body.get('reputation'), model.Reputation)
    save_store()
    return model


def response_from_model(model: PlayerModel, status_code: int = 200):
    return jsonify(asdict(model)), status_code


@app.before_request
def log_request() -> None:
    add_request_log({
        'method': request.method,
        'path': request.path,
        'query': request.query_string.decode('utf-8', errors='replace'),
        'remote_addr': request.headers.get('X-Forwarded-For', request.remote_addr),
    })


@app.get('/')
def index():
    return jsonify({
        'ok': True,
        'service': 'rec-room-mock-api',
        'message': 'Railway Flask mock API is running.',
        'player_api_examples': [
            '/api/players/v1?steamId=76561198000000000',
            '/api/players/v1/steam/76561198000000000',
        ],
    })


@app.get('/health')
def health():
    return jsonify({'ok': True})


@app.get('/__debug/requests')
def debug_requests():
    with REQUEST_LOCK:
        return jsonify({'count': len(request_log), 'items': request_log})


@app.route('/api/players/v1', methods=['GET', 'POST', 'PUT', 'PATCH'])
@app.route('/api/players/v1/', methods=['GET', 'POST', 'PUT', 'PATCH'])
@app.route('/api/players/v1/<path:subpath>', methods=['GET', 'POST', 'PUT', 'PATCH'])
def players_v1(subpath: str = ''):
    body = parse_body()
    steam_id = get_steam_id(subpath)

    with STORE_LOCK:
        if request.method == 'GET':
            model = players.get(steam_id)
            if model is None and AUTO_CREATE_ON_GET:
                model = get_or_create_player(steam_id, body)
            if model is None:
                return jsonify({'error': 'Player not found', 'steamId': steam_id}), 404
            return response_from_model(model)

        if request.method == 'POST':
            model = get_or_create_player(steam_id, body)
            model = update_player(model, body)
            return response_from_model(model, 201)

        if request.method in {'PUT', 'PATCH'}:
            model = get_or_create_player(steam_id, body)
            model = update_player(model, body)
            return response_from_model(model)

    return jsonify({'error': 'Unsupported method'}), 405


@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
def fallback(path: str):
    body = parse_body()
    return jsonify({
        'ok': True,
        'message': 'Unhandled path reached mock fallback.',
        'method': request.method,
        'path': '/' + path,
        'query': request.args.to_dict(flat=True),
        'body': body,
    }), 200


load_store()


if __name__ == '__main__':
    port = int(os.getenv('PORT', '8000'))
    app.run(host='0.0.0.0', port=port, debug=False)
