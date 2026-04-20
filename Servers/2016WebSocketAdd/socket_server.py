
from __future__ import annotations

import asyncio
import json
import os
import signal
import uuid
from typing import Any

from websockets.asyncio.server import serve
from websockets.datastructures import Headers


from rr23_shared import (
    auth_header_valid,
    list_ws_events_since,
    log_request,
    parse_bool,
    remove_ws_session,
    safe_int,
    touch_ws_session,
)

WS_HOST = os.environ.get("WS_HOST", "0.0.0.0")
WS_PORT = int(os.environ.get("WS_PORT", "8765"))
REQUIRE_WS_AUTH = parse_bool(os.environ.get("REQUIRE_WS_AUTH", "false"))
WS_IDLE_PING_SECONDS = max(5, int(os.environ.get("WS_IDLE_PING_SECONDS", "20")))
WS_EVENT_POLL_SECONDS = max(1, int(os.environ.get("WS_EVENT_POLL_SECONDS", "1")))

STOP_EVENT = asyncio.Event()


async def event_pump(websocket: Any, player_id: int, session_id: str) -> None:
    last_event_id = 0
    while True:
        touch_ws_session(player_id, session_id)
        events = list_ws_events_since(player_id, last_event_id)
        for event in events:
            await websocket.send(json.dumps({"Id": int(event["NotificationId"]), "Msg": event["Payload"]}, separators=(",", ":")))
            last_event_id = max(last_event_id, safe_int(event["EventId"], 0))
        await asyncio.sleep(WS_EVENT_POLL_SECONDS)


def is_authorized(headers: Headers) -> bool:
    if not REQUIRE_WS_AUTH:
        return True
    authorization = headers.get("Authorization")
    return auth_header_valid(authorization)


async def notification_handler(websocket: Any) -> None:
    path = getattr(getattr(websocket, "request", None), "path", "")
    headers = getattr(getattr(websocket, "request", None), "headers", Headers())
    log_request("WS", path or "/api/notification/v1", {}, 101, "connect-attempt")

    normalized_path = path.split("?", 1)[0]
    if normalized_path not in {"/api/notification", "/api/notification/v1", "/api/notification/v2"}:
        log_request('WS', normalized_path or '/unknown', {}, 404, 'wrong-ws-path')
        await websocket.close(code=1008, reason="wrong path")
        return

    if not is_authorized(headers):
        await websocket.close(code=1008, reason="unauthorized")
        return

    session_id = uuid.uuid4().hex
    player_id = 0
    pump_task: asyncio.Task[Any] | None = None
    try:
        handshake = await asyncio.wait_for(websocket.recv(), timeout=10)
        if not isinstance(handshake, str):
            await websocket.close(code=1003, reason="invalid handshake")
            return
        parsed = json.loads(handshake)
        if not isinstance(parsed, dict):
            await websocket.close(code=1003, reason="invalid handshake")
            return
        player_id = safe_int(
            parsed.get("PlayerId", parsed.get("playerId", parsed.get("ProfileId", parsed.get("profileId", parsed.get("Id", parsed.get("id", 0)))))),
            0,
        )
        if player_id <= 0:
            await websocket.close(code=1008, reason="missing player id")
            return

        touch_ws_session(player_id, session_id)
        if normalized_path == "/api/notification/v2":
            session_numeric_id = (uuid.uuid4().int % 2147483647) or 1
            await websocket.send(json.dumps({"SessionId": session_numeric_id}, separators=(",", ":")))
        else:
            await websocket.send("OK")
        pump_task = asyncio.create_task(event_pump(websocket, player_id, session_id))

        while True:
            message = await websocket.recv()
            if message is None:
                break
            touch_ws_session(player_id, session_id)
    except asyncio.CancelledError:
        raise
    except Exception:
        pass
    finally:
        if pump_task is not None:
            pump_task.cancel()
            try:
                await pump_task
            except Exception:
                pass
        if player_id > 0:
            remove_ws_session(player_id, session_id)
        log_request("WS", path or "/api/notification/v1", {}, 1000, "disconnect")


async def main() -> None:
    async with serve(
        notification_handler,
        WS_HOST,
        WS_PORT,
        ping_interval=WS_IDLE_PING_SECONDS,
        ping_timeout=WS_IDLE_PING_SECONDS,
        max_size=2 * 1024 * 1024,
    ):
        print(f"[socket_server] listening on {WS_HOST}:{WS_PORT}", flush=True)
        await STOP_EVENT.wait()


def _request_stop(*_: Any) -> None:
    STOP_EVENT.set()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    asyncio.run(main())
