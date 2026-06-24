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
from pathlib import Path
from typing import Any

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


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


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
    player = _BASE._PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
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

    return await _SHARED.handle_http(request=request, route_path=route_path, context=context)


handle_websocket = _SHARED.handle_websocket
