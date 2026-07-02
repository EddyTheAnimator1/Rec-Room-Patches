"""7 January 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 1355637356417786081:
- The HTTP/WebSocket route surface still matches the late-December 2016 family.
- Startup probes api/versioncheck/v1, creates/loads the local profile through
  api/players/v1/getorcreate, and downloads api/config/v2.
- This adapter also accepts the later platform-login/auth bootstrap shape so
  clients that ask for auth before local profile setup still receive a local
  token and player id.
- Local-player routes use X-Rec-Room-Profile or the local bearer token and the
  v2/v3 endpoint family.
- Game session lookups use api/gamesessions/v1/ and api/gamesessions/v1/<Id>.
- Push notifications use api/notification/v2.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response

API_VERSION = "7january2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Sat, 07 Jan 2017 02:41:28 GMT"


def _load_shared_adapter():
    module_path = Path(__file__).with_name("9december2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_9december2016_shared_for_7january2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 9december2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    module._BASE.API_VERSION = API_VERSION
    module._BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    module._BASE._set_api_version(module._BASE)
    return module


_SHARED = _load_shared_adapter()
_BASE = _SHARED._BASE
_PLATFORM_BASE = _SHARED._PLATFORM_BASE

_AUTH_BOOTSTRAP_PATHS = {
    "api/platformlogin/v1",
    "api/platformlogin/v1/",
    "api/auth/v1",
    "api/auth/v1/",
    "api/authentication/v1",
    "api/authentication/v1/",
}
_BEARER_RE = re.compile(rf"Bearer\s+local-{re.escape(API_VERSION)}-(\d+)", flags=re.IGNORECASE)


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


def _token_for_player_id(player_id: int) -> str:
    return f"local-{API_VERSION}-{player_id}"


def _player_id_from_authorization(request: Request) -> int:
    auth = request.headers.get("Authorization") or request.headers.get("authorization") or ""
    match = _BEARER_RE.fullmatch(auth.strip())
    return int(match.group(1)) if match else 0


def _local_profile_id(request: Request) -> int:
    raw_id = request.headers.get("X-Rec-Room-Profile") or request.headers.get("x-rec-room-profile")
    try:
        player_id = int(raw_id or 0)
    except Exception:
        player_id = 0
    if player_id > 0:
        return player_id

    player_id = _player_id_from_authorization(request)
    if player_id > 0:
        return player_id

    raise HTTPException(status_code=400, detail="X-Rec-Room-Profile or local bearer token is required.")


def _ensure_local_profile(request: Request, context) -> int:
    player_id = _local_profile_id(request)
    _BASE._ensure_existing_profile(context, player_id)
    return player_id


# The shared 9 December adapter looks up these functions dynamically from its
# module globals. Retarget them for this adapter so delegated routes also accept
# the 7 January local bearer token instead of only X-Rec-Room-Profile.
_SHARED._local_profile_id = _local_profile_id
_SHARED._ensure_local_profile = _ensure_local_profile


def _profile_payload(player: dict[str, Any]) -> dict[str, Any]:
    return _BASE._serialize_profile_for_recnet(player)


def _profile_response(player: dict[str, Any], *, status_code: int = 200) -> Response:
    return JSONResponse(_profile_payload(player), status_code=status_code)


async def _handle_local_profile(request: Request, context) -> Response:
    player_id = _local_profile_id(request)
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    return _profile_response(player)


async def _handle_auth_bootstrap(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    platform_id = _BASE._str_field(payload, "PlatformId", "platformId", "platform_id", "AuthParams", "authParams")
    if not platform_id:
        raise HTTPException(status_code=400, detail="PlatformId is required.")
    platform = _BASE._int_field(payload, "Platform", "platform", default=0)
    name = _BASE._str_field(payload, "Name", "name", "DisplayName", "displayName", default=f"Player{platform_id[-4:]}")

    context.assert_identities_not_banned(
        [
            ("account_id", f"steam:{platform_id}"),
            ("account_id", f"{API_VERSION}:platform:{platform}:{platform_id}"),
        ]
    )

    response = await _BASE._handle_create_profile(request, "api/players/v1/create", context)
    profile = _BASE._load_response_json(response)
    if not isinstance(profile, dict):
        raise HTTPException(status_code=500, detail="Auth bootstrap profile creation failed.")
    legacy_id = int(profile.get("Id") or profile.get("PlayerId") or 0)
    if legacy_id <= 0:
        raise HTTPException(status_code=500, detail="Auth bootstrap did not allocate a player id.")

    player = _PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
    if player is None:
        raise HTTPException(status_code=500, detail="Auth bootstrap player lookup failed.")
    context.assert_player_not_banned(player["player_id"])

    token = _token_for_player_id(legacy_id)
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
    return JSONResponse(
        {
            "Token": token,
            "PlayerId": legacy_id,
            "Player": _profile_payload(player),
        }
    )


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _clean_route_path(route_path).casefold()
    method = request.method.upper()

    if path in _AUTH_BOOTSTRAP_PATHS:
        if method != "POST":
            raise HTTPException(status_code=501, detail="Auth bootstrap method is not implemented.")
        return await _handle_auth_bootstrap(request, context)

    if path in {"api/players/v1", "api/players/v1/"}:
        if method != "GET":
            raise HTTPException(status_code=501, detail="Player profile method is not implemented.")
        return await _handle_local_profile(request, context)

    return await _SHARED.handle_http(request=request, route_path=route_path, context=context)


handle_websocket = _SHARED.handle_websocket
