"""21 September 2016 Rec Room HTTP API adapter.

Confirmed from decompiled client build 7007068986923462121, with the same
Assembly-CSharp.dll also present in build 386252387837028876:
- GET  api/players/v1/?p=<platform>&id=<platform id>
- GET  api/players/v1/<Id>
- POST api/players/v1/create with form fields Platform, PlatformId, and Name
- POST api/players/v1/update/<Id> with WebManager.PlayerModel JSON
- POST api/players/v1/verify/<Id> with form field email
- GET  api/images/v1/profile/<Id>
- POST api/images/v1/profile/<Id> with multipart form field image
- GET  motd, for patched MOTD URL convenience
- GET  api/tournament?player=<Photon player name>
- GET  api/tournament/forfeit?match=<MatchId>&player=<Photon player name>

No non-Photon WebSocket API was found. The player/image/MOTD/tournament
surface otherwise matches the 15 September adapter.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse, Response

API_VERSION = "21september2016"


def _load_base_adapter():
    module_path = Path(__file__).with_name("15september2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_15september2016_shared_for_21september2016", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 15september2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module._BASE.API_VERSION = API_VERSION
    module._PLATFORM_BASE.API_VERSION = API_VERSION
    return module


_BASE = _load_base_adapter()
_PLATFORM_BASE = _BASE._PLATFORM_BASE


def _route(route_path: str) -> str:
    return route_path.strip("/").casefold()


def _str_field(payload: dict[str, Any], *names: str, default: str = "") -> str:
    for name in names:
        if name in payload and payload[name] is not None:
            return str(payload[name]).strip()
    return default


async def _handle_verify_player(request: Request, route_path: str, context) -> Response:
    match = re.fullmatch(r"api/players/v1/verify/(\d+)/?", route_path.strip("/"), flags=re.IGNORECASE)
    if not match:
        raise HTTPException(status_code=404, detail="Unknown endpoint.")
    legacy_id = int(match.group(1))
    player = _PLATFORM_BASE._find_player_by_legacy_id(context, legacy_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])

    payload = await _PLATFORM_BASE._parse_client_payload(request)
    email = _str_field(payload, "email", "Email", default="")
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="email is required.")

    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET email = ?,
                verified = 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND is_coach = 0
            """,
            (email[:254], player["player_id"]),
        )
    return JSONResponse({"Message": "Registration email sent."})


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = _route(route_path)
    if path.startswith("api/players/v1/verify/"):
        if request.method != "POST":
            raise HTTPException(status_code=501, detail="Player verify method is not implemented.")
        return await _handle_verify_player(request, route_path, context)
    return await _BASE.handle_http(request=request, route_path=route_path, context=context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await _BASE.handle_websocket(websocket=websocket, route_path=route_path, context=context)
