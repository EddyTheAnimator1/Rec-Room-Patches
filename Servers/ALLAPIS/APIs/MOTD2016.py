"""Shared MOTD API for 2016 Rec Room builds.

Patched clients call:
  GET /MOTD2016/motd

Response is plain text consumed by IntroActivityManager via WWW.text.
"""

from __future__ import annotations

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import PlainTextResponse, Response


API_VERSION = "MOTD2016"


def serialize_motd_for_client(message: str) -> str:
    return message


def handle_http(*, request: Request, route_path: str, context) -> Response:
    path = route_path.strip("/")

    if request.method == "GET" and path == "motd":
        message = serialize_motd_for_client(context.get_motd(API_VERSION))
        return PlainTextResponse(message, media_type="text/plain; charset=utf-8")

    raise HTTPException(status_code=404, detail="Unknown endpoint.")


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    await websocket.close(code=1008)
