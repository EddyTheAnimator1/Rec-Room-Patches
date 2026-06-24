"""9 March 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from the game build at manifest 512603081605663477:
- RecNet core moved to COGCNMJCNKN.
- HTTP/WebSocket URL fields are EHBCBOGDLDB and FPGKGDJLOJJ.
- Login now posts a real /api/platformlogin/v1 form.
- The login form fields are Platform, PlatformId, Name, ClientTimestamp,
  BuildTimestamp, AuthParams, and Verify.
- Player subscription synchronization remains notification-WebSocket driven;
  REST api/PlayerSubscriptions/v1/init/add/remove is not a real route here.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response

API_VERSION = "9march2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Thu, 09 Mar 2017 02:36:59 GMT"


def _retarget_module(module) -> None:
    module.API_VERSION = API_VERSION
    module.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    if hasattr(module, "_BASE"):
        module._BASE.API_VERSION = API_VERSION
        module._BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
        module._BASE._set_api_version(module._BASE)
    if hasattr(module, "_SHARED"):
        _retarget_module(module._SHARED)


def _load_shared_adapter():
    module_path = Path(__file__).with_name("1march2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_1march2017_shared_for_9march2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 1march2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


def _find_attr(module, attr: str):
    current = module
    while current is not None:
        if hasattr(current, attr):
            return getattr(current, attr)
        current = getattr(current, "_SHARED", None)
    raise RuntimeError(f"Shared adapter does not expose {attr}.")


_SHARED = _load_shared_adapter()
_BASE = _find_attr(_SHARED, "_BASE")
_PLATFORM_BASE = _find_attr(_BASE, "_PLATFORM_BASE")
PROFILE_REQUIRED_KEYS = {"Id", "Username", "DisplayName", "XP", "Level", "Reputation", "Verified"}
PROFILE_DEFAULTS = {
    "Developer": True,
    "HasEmail": True,
    "CanReceiveInvites": True,
    "PhoneLastFour": "",
}


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _looks_like_profile_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and PROFILE_REQUIRED_KEYS.issubset(payload)


def _add_9march_profile_fields(payload: Any) -> bool:
    if _looks_like_profile_payload(payload):
        changed = False
        for key, value in PROFILE_DEFAULTS.items():
            if key not in payload:
                payload[key] = value
                changed = True
        return changed
    if isinstance(payload, list):
        changed = False
        for item in payload:
            changed = _add_9march_profile_fields(item) or changed
        return changed
    if isinstance(payload, dict) and "Player" in payload:
        return _add_9march_profile_fields(payload["Player"])
    return False


def _profile_payload(player: dict[str, Any]) -> dict[str, Any]:
    payload = _BASE._serialize_profile_for_recnet(player)
    _add_9march_profile_fields(payload)
    return payload


def _profile_response(player: dict[str, Any], *, status_code: int = 200) -> Response:
    return JSONResponse(_profile_payload(player), status_code=status_code)


def _augment_profile_response(response: Response) -> Response:
    payload = _BASE._load_response_json(response)
    if not _add_9march_profile_fields(payload):
        return response
    return JSONResponse(payload, status_code=getattr(response, "status_code", 200))


def _local_profile_id(request: Request) -> int:
    raw_id = request.headers.get("X-Rec-Room-Profile") or request.headers.get("x-rec-room-profile")
    try:
        player_id = int(raw_id or 0)
    except Exception:
        player_id = 0
    if player_id > 0:
        return player_id

    auth = request.headers.get("Authorization") or request.headers.get("authorization") or ""
    match = re.fullmatch(r"Bearer\s+local-9march2017-(\d+)", auth.strip(), flags=re.IGNORECASE)
    if match:
        return int(match.group(1))

    raise HTTPException(status_code=400, detail="X-Rec-Room-Profile is required.")


async def _handle_local_profile(request: Request, context) -> Response:
    player_id = _local_profile_id(request)
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    return _profile_response(player)


async def _handle_profiles_by_platform_ids(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    platform = _BASE._int_field(payload, "Platform", "platform", default=0)
    platform_ids = payload.get("PlatformIds") or payload.get("platformIds") or payload.get("platform_ids") or []
    if not isinstance(platform_ids, list):
        raise HTTPException(status_code=400, detail="PlatformIds must be a list.")

    results: list[dict[str, Any]] = []
    for raw_platform_id in platform_ids:
        platform_id = str(raw_platform_id).strip()
        if not platform_id:
            continue
        player = _PLATFORM_BASE._find_player_by_platform(context, platform=platform, platform_id=platform_id)
        if player is None:
            continue
        context.assert_player_not_banned(player["player_id"])
        results.append(
            {
                "Platform": platform,
                "PlatformId": platform_id,
                "Player": _profile_payload(player),
            }
        )
    return JSONResponse(results)


async def _handle_profile_search(route_path: str, context) -> Response:
    match = re.fullmatch(r"api/players/v1/search/([^/]+)/?", _clean_route_path(route_path), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    term = unquote(match.group(1)).strip().casefold()
    if not term:
        return JSONResponse([])

    like_term = f"%{term}%"
    profiles: list[dict[str, Any]] = []
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players AS p
            JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND (
                    lower(p.username) LIKE ?
                 OR lower(p.display_name) LIKE ?
                 OR lower(json_extract(pvs.state_json, '$.name')) LIKE ?
              )
            ORDER BY p.updated_at DESC
            LIMIT 20
            """,
            (_PLATFORM_BASE.STATE_API_VERSION, like_term, like_term, like_term),
        ).fetchall()

    for row in rows:
        player = {key: row[key] for key in row.keys() if key != "state_json"}
        try:
            player["state"] = json.loads(row["state_json"] or "{}")
        except Exception:
            player["state"] = {}
        context.assert_player_not_banned(player["player_id"])
        profiles.append(_profile_payload(player))
    return JSONResponse(profiles)


async def _handle_platform_login(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    platform_id = _BASE._str_field(payload, "PlatformId", "platformId", "platform_id")
    if not platform_id:
        raise HTTPException(status_code=400, detail="PlatformId is required.")
    name = _BASE._str_field(payload, "Name", "name", default=f"Player{platform_id[-4:]}")
    platform = _BASE._int_field(payload, "Platform", "platform", default=0)

    context.assert_identities_not_banned(
        [
            ("account_id", f"steam:{platform_id}"),
            ("account_id", f"{API_VERSION}:platform:{platform}:{platform_id}"),
        ]
    )

    response = await _BASE._handle_create_profile(request, "api/players/v1/create", context)
    profile = _BASE._load_response_json(response)
    if not isinstance(profile, dict):
        raise HTTPException(status_code=500, detail="Platform login profile creation failed.")
    legacy_id = int(profile.get("Id") or profile.get("PlayerId") or 0)
    if legacy_id <= 0:
        raise HTTPException(status_code=500, detail="Platform login did not allocate a player id.")
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
    if player is None:
        raise HTTPException(status_code=500, detail="Platform login player lookup failed.")
    context.assert_player_not_banned(player["player_id"])
    token = f"local-{API_VERSION}-{legacy_id}"

    state = context.ensure_player_version_state(
        player["player_id"],
        API_VERSION,
        {
            "identity_key": f"platform:{platform}:{platform_id}",
            "platform": platform,
            "platform_id": platform_id,
        },
    )
    state.update(
        {
            "identity_key": f"platform:{platform}:{platform_id}",
            "platform": platform,
            "platform_id": platform_id,
            "name": name,
            "recnet_id": legacy_id,
            "login_token": token,
        }
    )
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )
    context.record_player_identities(
        player["player_id"],
        [
            ("account_id", f"steam:{platform_id}"),
            ("account_id", f"{API_VERSION}:platform:{platform}:{platform_id}"),
            ("account_id", f"recnet:{legacy_id}"),
            ("account_id", f"{API_VERSION}:recnet:{legacy_id}"),
            ("account_id", token),
        ],
    )
    return JSONResponse({"Token": token, "PlayerId": legacy_id})


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path in {"api/platformlogin/v1", "api/platformlogin/v1/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Platform login method is not implemented.")
        return await _handle_platform_login(request, context)

    if path.startswith("api/playersubscriptions/"):
        raise HTTPException(status_code=404, detail="Unknown endpoint.")

    if path in {"api/players/v1", "api/players/v1/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Player profile method is not implemented.")
        return await _handle_local_profile(request, context)

    if path in {"api/players/v1/listbyplatformid", "api/players/v1/listbyplatformid/"}:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Player platform list method is not implemented.")
        return await _handle_profiles_by_platform_ids(request, context)

    if path.startswith("api/players/v1/search/"):
        if method != "GET":
            raise HTTPException(status_code=501, detail="Player search method is not implemented.")
        return await _handle_profile_search(route_path, context)

    response = await _SHARED.handle_http(request=request, route_path=route_path, context=context)
    if path.startswith("api/players/"):
        return _augment_profile_response(response)
    return response


handle_websocket = _SHARED.handle_websocket
