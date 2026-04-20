from __future__ import annotations

from typing import Any


def start_background_workers() -> None:
    return None


def maybe_emit_periodic_snapshots() -> None:
    return None


def emit_presence_snapshot_now(force: bool = False) -> bool:
    return False


def emit_analytics_event(payload: dict[str, Any]) -> None:
    return None
