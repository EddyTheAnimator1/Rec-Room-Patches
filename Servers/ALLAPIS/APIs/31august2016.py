"""31 August 2016 Rec Room HTTP API adapter.

Confirmed from decompiled client build 3233729662911539772:
- GET  api/players/v1/?p=<platform>&id=<platform id>
- POST api/players/v1/create with form fields Platform, PlatformId, and Name
- POST api/players/v1/update/<Id> with WebManager.PlayerModel JSON
- GET  motd, for patched MOTD URL convenience
- GET  api/tournament?player=<Photon player name>
- GET  api/tournament/forfeit?match=<MatchId>&player=<Photon player name>

This build replaces the old SteamID-shaped player model with a platform-shaped
model. Tournament routes are safe no-active-match/no-op stubs.
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import parse_qs

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from starlette import status

API_VERSION = "31august2016"
STATE_API_VERSION = "2016_legacy_player"
NEXT_PLAYER_ID_SETTING = f"{STATE_API_VERSION}.next_legacy_player_id"
DEFAULT_REPUTATION = 0


def _route(route_path: str) -> str:
    return route_path.strip("/").casefold()


def _safe_display_name(value: Any, fallback: str) -> str:
    name = str(value or "").strip()
    if not name:
        name = fallback
    if name.casefold() == "coach":
        return "Player"
    return name[:64]


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
            (player_id, STATE_API_VERSION),
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
            (player_id, STATE_API_VERSION, json.dumps(state, sort_keys=True)),
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
            (STATE_API_VERSION,),
        ).fetchone()
        max_id = int(max_row["max_id"] or 0)
        next_id = max(next_id, max_id + 1, 1)
        conn.execute(
            """
            INSERT INTO server_settings(key, value_json, created_at, updated_at)
            VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
            """,
            (NEXT_PLAYER_ID_SETTING, json.dumps(next_id + 1)),
        )
        return next_id


def _platform_identity_key(platform: int, platform_id: str) -> str:
    if platform == 0:
        return f"steam:{platform_id}"
    return f"platform:{platform}:{platform_id}"


def _canonical_username(platform: int, platform_id: str) -> str:
    digits = re.sub(r"\D+", "", str(platform_id or ""))
    if platform == 0:
        return f"Steam_{digits or 'Unknown'}"
    if platform == 1:
        return f"Oculus_{digits or 'Unknown'}"
    return f"Platform{platform}_{digits or 'Unknown'}"


def _identity_pairs(platform: int, platform_id: str) -> list[tuple[str, str]]:
    pairs = [("account_id", _platform_identity_key(platform, platform_id))]
    pairs.append(("account_id", f"platform:{platform}:{platform_id}"))
    return pairs


def _ensure_platform_state(context, player: dict[str, Any], *, platform: int, platform_id: str, display_name: str) -> dict[str, Any]:
    state = _fetch_version_state(context, player["player_id"])
    changed = False
    identity_key = _platform_identity_key(platform, platform_id)
    for key, value in {
        "identity_key": identity_key,
        "platform": int(platform),
        "platform_id": str(platform_id),
    }.items():
        if state.get(key) != value:
            state[key] = value
            changed = True
    if platform == 0 and str(state.get("steam_id") or "") != str(platform_id):
        state["steam_id"] = str(platform_id)
        changed = True
    if not state.get("legacy_player_id"):
        state["legacy_player_id"] = _allocate_legacy_player_id(context)
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


def _find_player_by_platform(context, *, platform: int, platform_id: str) -> dict[str, Any] | None:
    player = context.find_player_by_identity("account_id", _platform_identity_key(platform, platform_id))
    if player is None:
        player = context.find_player_by_identity("account_id", f"platform:{platform}:{platform_id}")
    if player is None:
        with context.db.connection() as conn:
            row = conn.execute(
                """
                SELECT p.*, pvs.state_json
                FROM players AS p
                JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
                WHERE pvs.api_version = ?
                  AND CAST(json_extract(pvs.state_json, '$.platform') AS INTEGER) = ?
                  AND json_extract(pvs.state_json, '$.platform_id') = ?
                ORDER BY pvs.updated_at DESC
                LIMIT 1
                """,
                (STATE_API_VERSION, platform, str(platform_id)),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT p.*, pvs.state_json
                    FROM players AS p
                    JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
                    WHERE CAST(json_extract(pvs.state_json, '$.platform') AS INTEGER) = ?
                      AND json_extract(pvs.state_json, '$.platform_id') = ?
                    ORDER BY pvs.updated_at DESC
                    LIMIT 1
                    """,
                    (platform, str(platform_id)),
                ).fetchone()
        if row is None:
            return None
        player = {key: row[key] for key in row.keys() if key != "state_json"}
    state = _ensure_platform_state(
        context,
        player,
        platform=platform,
        platform_id=str(platform_id),
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
            (STATE_API_VERSION, legacy_id),
        ).fetchone()
        if row is None:
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


def _create_player_for_platform(context, *, platform: int, platform_id: str, name: str) -> dict[str, Any]:
    fallback = f"Player{platform_id[-4:] if platform_id else ''}"
    display_name = _safe_display_name(name, fallback=fallback)
    canonical = context.get_or_create_player(
        STATE_API_VERSION,
        identity_key=_platform_identity_key(platform, platform_id),
        username=_canonical_username(platform, platform_id),
        display_name=display_name,
    )
    state = _ensure_platform_state(context, canonical, platform=platform, platform_id=platform_id, display_name=display_name)
    canonical["state"] = state
    return canonical


def _serialize_player_for_client(player: dict[str, Any]) -> dict[str, Any]:
    state = player.get("state") or {}
    return {
        "Id": int(state.get("legacy_player_id") or 0),
        "Platform": int(state.get("platform") or 0),
        "PlatformId": int(str(state.get("platform_id") or "0") or 0),
        "Name": str(player.get("display_name") or player.get("username") or state.get("name") or "Player"),
        "XP": int(player.get("canonical_xp") or 0),
        "Level": max(1, int(player.get("canonical_level") or 1)),
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
    platform = _int_field(dict(request.query_params), "p", "Platform", "platform", default=0)
    platform_id = str(request.query_params.get("id") or request.query_params.get("PlatformId") or "").strip()
    if not platform_id:
        raise HTTPException(status_code=400, detail="id is required.")
    player = _find_player_by_platform(context, platform=platform, platform_id=platform_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    context.record_player_identities(player["player_id"], _identity_pairs(platform, platform_id))
    return _json_response(player)


async def _handle_create_player(request: Request, context) -> Response:
    payload = await _parse_client_payload(request)
    platform = _int_field(payload, "Platform", "platform", "p", default=0)
    platform_id = _str_field(payload, "PlatformId", "platformId", "platform_id", "id")
    if not platform_id:
        raise HTTPException(status_code=400, detail="PlatformId is required.")
    context.assert_identities_not_banned(_identity_pairs(platform, platform_id))
    existing = _find_player_by_platform(context, platform=platform, platform_id=platform_id)
    if existing is not None:
        return _json_response(existing)
    name = _str_field(payload, "Name", "name", default=f"Player{platform_id[-4:]}")
    player = _create_player_for_platform(context, platform=platform, platform_id=platform_id, name=name)
    context.record_player_identities(player["player_id"], _identity_pairs(platform, platform_id))
    return _json_response(player, status_code=status.HTTP_201_CREATED)


async def _handle_update_player(request: Request, route_path: str, context) -> Response:
    match = re.fullmatch(r"api/players/v1/update/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    legacy_id = int(match.group(1))
    player = _find_player_by_legacy_id(context, legacy_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    payload = await _parse_client_payload(request)
    state = player.get("state") or {}
    state["name"] = _safe_display_name(
        _str_field(payload, "Name", "name", default=str(state.get("name") or player.get("display_name") or "Player")),
        fallback=str(state.get("name") or "Player"),
    )
    state["platform"] = _int_field(payload, "Platform", "platform", default=int(state.get("platform") or 0))
    state["platform_id"] = _str_field(payload, "PlatformId", "platformId", "platform_id", default=str(state.get("platform_id") or "0"))
    state["reputation"] = _int_field(payload, "Reputation", "reputation", default=int(state.get("reputation") or DEFAULT_REPUTATION))
    xp = _int_field(payload, "XP", "xp", default=int(player.get("canonical_xp") or 0))
    level = _int_field(payload, "Level", "level", default=int(player.get("canonical_level") or 1))
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET display_name = ?, canonical_xp = ?, canonical_level = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND is_coach = 0
            """,
            (state["name"], max(0, xp), max(1, level), player["player_id"]),
        )
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], STATE_API_VERSION),
        )
    updated = _find_player_by_legacy_id(context, legacy_id)
    return _json_response(updated or player)


def _handle_motd(context) -> Response:
    return PlainTextResponse(context.get_motd(API_VERSION), media_type="text/plain; charset=utf-8")


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _route(route_path)
    if request.method == "GET" and path == "motd":
        return _handle_motd(context)
    if request.method == "GET" and path in {"api/players/v1", "api/players/v1/"}:
        return await _handle_get_player(request, context)
    if path in {"api/players/v1/create", "api/players/v1/create/"}:
        if request.method != "POST":
            raise HTTPException(status_code=501, detail="Player create method is not implemented.")
        return await _handle_create_player(request, context)
    if path.startswith("api/players/v1/update/"):
        if request.method != "POST":
            raise HTTPException(status_code=501, detail="Player update method is not implemented.")
        return await _handle_update_player(request, route_path, context)
    if path in {"api/tournament", "api/tournament/"}:
        if request.method != "GET":
            raise HTTPException(status_code=501, detail="Tournament method is not implemented.")
        return Response(status_code=204)
    if path in {"api/tournament/forfeit", "api/tournament/forfeit/"}:
        if request.method != "GET":
            raise HTTPException(status_code=501, detail="Tournament forfeit method is not implemented.")
        return Response(status_code=204)
    raise HTTPException(status_code=404, detail="Unknown endpoint.")


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await websocket.close(code=1008)
