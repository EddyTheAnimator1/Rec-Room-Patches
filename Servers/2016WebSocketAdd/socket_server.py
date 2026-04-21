
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
    connect,
    get_latest_ws_event_id,
    init_db,
    log_request,
    parse_bool,
    remove_ws_session,
    safe_int,
    touch_ws_session,
    utcnow_iso,
)

WS_HOST = os.environ.get("WS_HOST", "0.0.0.0")
WS_PORT = int(os.environ.get("WS_PORT", "8765"))
REQUIRE_WS_AUTH = parse_bool(os.environ.get("REQUIRE_WS_AUTH", "false"))
WS_IDLE_PING_SECONDS = max(5, int(os.environ.get("WS_IDLE_PING_SECONDS", "20")))
WS_EVENT_POLL_SECONDS = max(0.05, float(os.environ.get("WS_EVENT_POLL_SECONDS", "0.10")))
WS_SESSION_TOUCH_SECONDS = max(5.0, float(os.environ.get("WS_SESSION_TOUCH_SECONDS", "15")))
WS_DB_RETRY_SECONDS = max(0.10, float(os.environ.get("WS_DB_RETRY_SECONDS", "0.50")))

STOP_EVENT = asyncio.Event()


def _touch_ws_session_with_conn(conn: Any, player_id: int, session_id: str) -> None:
    now_text = utcnow_iso()
    conn.execute(
        """
        INSERT INTO websocket_sessions(player_id, session_id, connected_at, last_seen_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(player_id, session_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
        """,
        (player_id, session_id, now_text, now_text),
    )
    conn.commit()


def _list_ws_events_since_with_conn(conn: Any, player_id: int, after_event_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, notification_id, payload_json, created_at
        FROM websocket_events
        WHERE player_id = ? AND id > ?
        ORDER BY id
        """,
        (player_id, after_event_id),
    ).fetchall()
    events: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"]))
        except Exception:
            payload = str(row["payload_json"])
        events.append({
            "EventId": safe_int(row["id"], 0),
            "NotificationId": safe_int(row["notification_id"], 0),
            "Payload": payload,
            "CreatedAt": str(row["created_at"]),
        })
    return events


async def event_pump(websocket: Any, player_id: int, session_id: str, initial_last_event_id: int) -> None:
    init_db()
    last_event_id = max(0, safe_int(initial_last_event_id, 0))
    conn: Any | None = None
    last_touch_monotonic = 0.0
    loop = asyncio.get_running_loop()
    try:
        while True:
            try:
                if conn is None:
                    conn = connect()
                    last_touch_monotonic = 0.0

                now_monotonic = loop.time()
                if now_monotonic - last_touch_monotonic >= WS_SESSION_TOUCH_SECONDS:
                    _touch_ws_session_with_conn(conn, player_id, session_id)
                    last_touch_monotonic = now_monotonic

                events = _list_ws_events_since_with_conn(conn, player_id, last_event_id)
                if events:
                    for event in events:
                        await websocket.send(json.dumps({"Id": int(event["NotificationId"]), "Msg": event["Payload"]}, separators=(",", ":")))
                        last_event_id = max(last_event_id, safe_int(event["EventId"], 0))
                    continue
            except asyncio.CancelledError:
                raise
            except Exception:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                conn = None
                await asyncio.sleep(WS_DB_RETRY_SECONDS)
                continue

            await asyncio.sleep(WS_EVENT_POLL_SECONDS)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


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
        last_event_id = get_latest_ws_event_id(player_id)
        if normalized_path == "/api/notification/v2":
            session_numeric_id = (uuid.uuid4().int % 2147483647) or 1
            await websocket.send(json.dumps({"SessionId": session_numeric_id}, separators=(",", ":")))
        else:
            await websocket.send("OK")
        pump_task = asyncio.create_task(event_pump(websocket, player_id, session_id, last_event_id))

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
