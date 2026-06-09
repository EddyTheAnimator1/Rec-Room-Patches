"""11 August 2016 v1 Rec Room HTTP API adapter.

Confirmed from decompiled client build 2459083052223685832:
- GET  api/players/?steamId=<ulong>
- POST api/players/ with WWWForm fields SteamID and Name
- PUT  api/players/<Id> with PlayerModel JSON
- GET  motd, for patched MOTD URL convenience

No non-Photon WebSocket API was found in this build.
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import parse_qs

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from starlette import status

API_VERSION = "11august2016v1"
NEXT_PLAYER_ID_SETTING = f"{API_VERSION}.next_legacy_player_id"
DEFAULT_EMAIL = "idontwanttoguess@gmail.com"
DEFAULT_GENDER = ""
DEFAULT_REPUTATION = 0


def _route(route_path: str) -> str:
    return route_path.strip("/").casefold()


def _safe_display_name(value: Any, fallback: str) -> str:
    name = str(value or "").strip()
    if not name:
        name = fallback
    # Coach is a reserved system identity. The 2016 client only needs a display name.
    if name.casefold() == "coach":
        return "Player"
    return name[:64]


def _canonical_username_for_steam(steam_id: str) -> str:
    digits = re.sub(r"\D+", "", str(steam_id or ""))
    return f"Steam_{digits or 'Unknown'}"


def _state_from_row(row: Any | None) -> dict[str, Any]:
    if row is None:
        return {}
    try:
        state = json.loads(row["state_json"] or "{}")
    except Exception:
        state = {}
    return state if isinstance(state, dict) else {}


def _fetch_version_state(context, player_id: str) -> dict[str, Any]:
    with context.db.connection() as conn:
        row = conn.execute(
            "SELECT state_json FROM player_version_state WHERE player_id = ? AND api_version = ?",
            (player_id, API_VERSION),
        ).fetchone()
    return _state_from_row(row)


def _save_version_state(context, player_id: str, state: dict[str, Any]) -> None:
    with context.db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO player_version_state(player_id, api_version, state_json, created_at, updated_at)
            VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            ON CONFLICT(player_id, api_version) DO UPDATE
            SET state_json = excluded.state_json, updated_at = excluded.updated_at
            """,
            (player_id, API_VERSION, json.dumps(state, sort_keys=True)),
        )


def _allocate_legacy_player_id(context) -> int:
    with context.db.transaction() as conn:
        row = conn.execute("SELECT value_json FROM server_settings WHERE key = ?", (NEXT_PLAYER_ID_SETTING,)).fetchone()
        try:
            next_id = int(json.loads(row["value_json"])) if row else 1
        except Exception:
            next_id = 1
        max_row = conn.execute(
            """
            SELECT MAX(CAST(json_extract(state_json, '$.legacy_player_id') AS INTEGER)) AS max_id
            FROM player_version_state
            WHERE api_version = ?
            """,
            (API_VERSION,),
        ).fetchone()
        max_id = int(max_row["max_id"] or 0)
        next_id = max(next_id, max_id + 1, 1)
        new_next = next_id + 1
        conn.execute(
            """
            INSERT INTO server_settings(key, value_json, created_at, updated_at)
            VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
            """,
            (NEXT_PLAYER_ID_SETTING, json.dumps(new_next)),
        )
        return next_id


def _find_player_by_steam_id(context, steam_id: str) -> dict[str, Any] | None:
    """Find by the shared canonical Steam identity, then ensure this build's state.

    Steam ID is not owned by one API version. It is a canonical account identity
    that should point to the same player everywhere. The 2016-specific legacy
    integer ID remains version adapter state because this old client requires it.
    """
    identity_key = f"steam:{steam_id}"
    player = context.find_player_by_identity("account_id", identity_key)
    if player is None:
        with context.db.connection() as conn:
            row = conn.execute(
                """
                SELECT p.*, pvs.state_json
                FROM players AS p
                JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
                WHERE json_extract(pvs.state_json, '$.steam_id') = ?
                   OR json_extract(pvs.state_json, '$.identity_key') = ?
                ORDER BY pvs.updated_at DESC
                LIMIT 1
                """,
                (str(steam_id), identity_key),
            ).fetchone()
        if row is None:
            return None
        player = {key: row[key] for key in row.keys() if key != "state_json"}
    state = _ensure_legacy_state(
        context,
        player,
        steam_id=str(steam_id),
        display_name=str(player.get("display_name") or player.get("username") or "Player"),
    )
    player["state"] = state
    return player


def _find_player_by_legacy_id(context, legacy_id: int) -> dict[str, Any] | None:
    with context.db.connection() as conn:
        row = conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players AS p
            JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND CAST(json_extract(pvs.state_json, '$.legacy_player_id') AS INTEGER) = ?
            LIMIT 1
            """,
            (API_VERSION, legacy_id),
        ).fetchone()
    if row is None:
        return None
    player = {key: row[key] for key in row.keys() if key != "state_json"}
    player["state"] = _state_from_row(row)
    return player


def _ensure_legacy_state(context, player: dict[str, Any], *, steam_id: str, display_name: str) -> dict[str, Any]:
    state = _fetch_version_state(context, player["player_id"])
    changed = False
    if not state.get("identity_key"):
        state["identity_key"] = f"steam:{steam_id}"
        changed = True
    if str(state.get("steam_id") or "") != str(steam_id):
        state["steam_id"] = str(steam_id)
        changed = True
    if not state.get("legacy_player_id"):
        state["legacy_player_id"] = _allocate_legacy_player_id(context)
        changed = True
    if not state.get("email"):
        state["email"] = player.get("email") or DEFAULT_EMAIL
        changed = True
    if "gender" not in state:
        state["gender"] = DEFAULT_GENDER
        changed = True
    if "reputation" not in state:
        state["reputation"] = DEFAULT_REPUTATION
        changed = True
    if display_name and state.get("name") != display_name:
        state["name"] = display_name
        changed = True
    if changed:
        _save_version_state(context, player["player_id"], state)
    return state


def _create_player_for_steam(context, *, steam_id: str, name: str) -> dict[str, Any]:
    display_name = _safe_display_name(name, fallback=f"Player{steam_id[-4:] if steam_id else ''}")
    canonical = context.get_or_create_player(
        API_VERSION,
        identity_key=f"steam:{steam_id}",
        username=_canonical_username_for_steam(steam_id),
        display_name=display_name,
    )
    state = _ensure_legacy_state(context, canonical, steam_id=steam_id, display_name=display_name)
    canonical["state"] = state
    return canonical


def _serialize_player_for_client(player: dict[str, Any]) -> dict[str, Any]:
    state = player.get("state") or {}
    return {
        "Id": int(state.get("legacy_player_id") or 0),
        "Name": str(player.get("display_name") or player.get("username") or state.get("name") or "Player"),
        "SteamID": int(str(state.get("steam_id") or "0") or 0),
        "Email": str(player.get("email") or state.get("email") or DEFAULT_EMAIL),
        "Gender": str(state.get("gender") or DEFAULT_GENDER),
        "XP": int(player.get("canonical_xp") or 0),
        "Level": int(player.get("canonical_level") or 1),
        "Reputation": int(state.get("reputation") or DEFAULT_REPUTATION),
    }


async def _parse_client_payload(request: Request) -> dict[str, Any]:
    body = await request.body()
    if not body:
        return {}
    content_type = str(request.headers.get("content-type") or "").casefold()

    if "json" in content_type:
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Invalid JSON payload.") from exc
        return payload if isinstance(payload, dict) else {}

    if "x-www-form-urlencoded" in content_type:
        parsed = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
        return {key: values[-1] if values else "" for key, values in parsed.items()}

    if "multipart/form-data" in content_type:
        return _parse_multipart_fields(body, content_type)

    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    parsed = parse_qs(text, keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


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


def _json_response(player: dict[str, Any], status_code: int = 200) -> Response:
    return JSONResponse(_serialize_player_for_client(player), status_code=status_code)


async def _handle_get_player(request: Request, context) -> Response:
    steam_id = str(request.query_params.get("steamId") or request.query_params.get("SteamID") or "").strip()
    if not steam_id:
        raise HTTPException(status_code=400, detail="steamId is required.")
    player = _find_player_by_steam_id(context, steam_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    context.record_player_identities(player["player_id"], [("account_id", f"steam:{steam_id}")])
    return _json_response(player)


async def _handle_create_player(request: Request, context) -> Response:
    payload = await _parse_client_payload(request)
    steam_id = _str_field(payload, "SteamID", "steamId", "steam_id")
    if not steam_id:
        raise HTTPException(status_code=400, detail="SteamID is required.")
    context.assert_identities_not_banned([("account_id", f"steam:{steam_id}")])
    name = _str_field(payload, "Name", "name", default=f"Player{steam_id[-4:]}")
    existing = _find_player_by_steam_id(context, steam_id)
    if existing is not None:
        return _json_response(existing)
    player = _create_player_for_steam(context, steam_id=steam_id, name=name)
    context.record_player_identities(player["player_id"], [("account_id", f"steam:{steam_id}")])
    return _json_response(player, status_code=status.HTTP_201_CREATED)


async def _handle_update_player(request: Request, route_path: str, context) -> Response:
    match = re.fullmatch(r"api/players/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    legacy_id = int(match.group(1))
    player = _find_player_by_legacy_id(context, legacy_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])

    payload = await _parse_client_payload(request)
    state = player.get("state") or {}
    name = _str_field(payload, "Name", "name", default=str(state.get("name") or player.get("display_name") or "Player"))
    state["name"] = _safe_display_name(name, fallback=str(state.get("name") or "Player"))
    state["email"] = _str_field(payload, "Email", "email", default=str(player.get("email") or state.get("email") or DEFAULT_EMAIL)) or DEFAULT_EMAIL
    state["gender"] = _str_field(payload, "Gender", "gender", default=str(state.get("gender") or DEFAULT_GENDER))
    state["reputation"] = _int_field(payload, "Reputation", "reputation", default=int(state.get("reputation") or DEFAULT_REPUTATION))

    xp = _int_field(payload, "XP", "xp", default=int(player.get("canonical_xp") or 0))
    level = _int_field(payload, "Level", "level", default=int(player.get("canonical_level") or 1))
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET display_name = ?, email = ?, canonical_xp = ?, canonical_level = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND is_coach = 0
            """,
            (state["name"], state["email"], max(0, xp), max(1, level), player["player_id"]),
        )
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )

    updated = _find_player_by_legacy_id(context, legacy_id)
    return _json_response(updated or player)


def _handle_motd(context) -> Response:
    return PlainTextResponse(context.get_motd(API_VERSION), media_type="text/plain; charset=utf-8")


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _route(route_path)

    if request.method == "GET" and path == "motd":
        return _handle_motd(context)

    if request.method == "GET" and path in {"api/players", "api/players/"}:
        return await _handle_get_player(request, context)

    if request.method == "POST" and path in {"api/players", "api/players/"}:
        return await _handle_create_player(request, context)

    if request.method == "PUT" and path.startswith("api/players/"):
        return await _handle_update_player(request, route_path, context)

    raise HTTPException(status_code=404, detail="Unknown endpoint.")


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await websocket.close(code=1008)
