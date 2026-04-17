from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import rr23_shared as shared

MONITORING_ENABLED = os.environ.get("MONITORING_ENABLED", "false").strip().lower() in {"1", "true", "yes", "y", "on"}
MONITORING_NOTIFIER_URL = os.environ.get("MONITORING_NOTIFIER_URL", "").strip()
MONITORING_SHARED_SECRET = os.environ.get("MONITORING_SHARED_SECRET", "").strip()
MONITORING_TIMEOUT_SECONDS = max(1.0, float(os.environ.get("MONITORING_TIMEOUT_SECONDS", "1.5")))
MONITORING_SNAPSHOT_INTERVAL_SECONDS = max(60, int(os.environ.get("MONITORING_SNAPSHOT_INTERVAL_SECONDS", "300")))
MONITORING_STALE_TIMEOUT_SECONDS = max(60, int(os.environ.get("MONITORING_STALE_TIMEOUT_SECONDS", "180")))
MONITORING_HEALTH_WINDOW_SECONDS = max(60, int(os.environ.get("MONITORING_HEALTH_WINDOW_SECONDS", "300")))
SERVICE_NAME = os.environ.get("MONITORING_SERVICE_NAME", os.environ.get("RAILWAY_SERVICE_NAME", "rr23-2016"))
SERVICE_ENVIRONMENT = os.environ.get("MONITORING_SERVICE_ENVIRONMENT", os.environ.get("RAILWAY_ENVIRONMENT_NAME", "production"))
STATE_PATH = shared.DATA_DIR / 'monitoring_state.json'
_STATE_LOCK = threading.Lock()


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _save_state(state: dict[str, Any]) -> None:
    try:
        shared.ensure_data_dir()
        STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception:
        pass


def monitoring_enabled() -> bool:
    return MONITORING_ENABLED and bool(MONITORING_NOTIFIER_URL) and bool(MONITORING_SHARED_SECRET)


def _post_json(url: str, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode('utf-8')
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            'Content-Type': 'application/json; charset=utf-8',
            'X-RR-Monitoring-Secret': MONITORING_SHARED_SECRET,
        },
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=MONITORING_TIMEOUT_SECONDS) as resp:
        resp.read(1024)


def _source() -> dict[str, str]:
    return {
        'service': SERVICE_NAME,
        'environment': SERVICE_ENVIRONMENT,
    }


def send_event(event_type: str, severity: str, fingerprint: str, summary: str, details: dict[str, Any]) -> None:
    if not monitoring_enabled():
        return
    payload = {
        'schema_version': 1,
        'event_type': str(event_type),
        'severity': str(severity),
        'timestamp_utc': shared.utcnow_iso(),
        'source': _source(),
        'fingerprint': str(fingerprint),
        'summary': str(summary),
        'details': details,
    }
    try:
        _post_json(MONITORING_NOTIFIER_URL, payload)
    except Exception:
        pass


def safe_preview_dict(payload: dict[str, Any] | None, allowed_keys: list[str] | tuple[str, ...]) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    preview: dict[str, Any] = {}
    for key in allowed_keys:
        if key in source:
            value = source.get(key)
            if isinstance(value, (str, int, float, bool)) or value is None:
                preview[key] = value
    return preview


def emit_missing_http_endpoint(method: str, path: str, reason: str, *, status_code: int = 404, payload: dict[str, Any] | None = None, query_string: str = '', user_agent: str = '') -> None:
    preview = safe_preview_dict(payload, ['Id', 'PlayerId', 'ProfileId', 'GameSessionId', 'Category', 'Action', 'Label'])
    send_event(
        'missing_http_endpoint',
        'warning',
        f'missing_http:{method.upper()}:{path}',
        'A request arrived, but no implemented HTTP route answered this path.',
        {
            'method': method.upper(),
            'path': path,
            'status_code': int(status_code),
            'reason': str(reason),
            'query_string': str(query_string or ''),
            'client': {
                'kind': 'game_client' if 'Unity' in (user_agent or '') else 'unknown',
                'user_agent_hint': 'UnityPlayer' if 'Unity' in (user_agent or '') else (str(user_agent or '')[:80] or 'unknown'),
            },
            'payload_excerpt': {
                'keys': sorted(preview.keys()),
                'preview': preview,
            },
        },
    )


def emit_missing_ws_action(message_type: str, reason: str, *, payload: dict[str, Any] | None = None, status_code: int = 404) -> None:
    preview = safe_preview_dict(payload, ['PlayerId', 'Id', 'GameSessionId', 'Type'])
    send_event(
        'missing_ws_action',
        'warning',
        f'missing_ws:{message_type}',
        'A websocket message arrived, but no implemented handler accepted its action.',
        {
            'message_type': str(message_type),
            'status_code': int(status_code),
            'reason': str(reason),
            'payload_excerpt': {
                'keys': sorted(preview.keys()),
                'preview': preview,
            },
        },
    )


def emit_analytics_event(payload: dict[str, Any]) -> None:
    category = str(payload.get('Category', payload.get('category', '')) or '')
    action = str(payload.get('Action', payload.get('action', '')) or '')
    label = str(payload.get('Label', payload.get('label', '')) or '')
    send_event(
        'analytics_event',
        'info',
        f'analytics:{category}:{action}:{label}',
        'A client submitted an analytics event.',
        {
            'session_id_present': bool(payload.get('SessionId') or payload.get('sessionId')),
            'category': category,
            'action': action,
            'label': label,
            'value': payload.get('Value', payload.get('value')),
            'value2': payload.get('Value2', payload.get('value2')),
            'value3': payload.get('Value3', payload.get('value3')),
        },
    )


def maybe_emit_periodic_snapshots() -> None:
    if not monitoring_enabled():
        return
    now = time.time()
    with _STATE_LOCK:
        state = _load_state()
        last_presence = float(state.get('last_presence_snapshot_unix', 0.0) or 0.0)
        last_health = float(state.get('last_health_snapshot_unix', 0.0) or 0.0)
        if now - last_presence >= MONITORING_SNAPSHOT_INTERVAL_SECONDS:
            snapshot = shared.get_presence_snapshot(stale_timeout_seconds=MONITORING_STALE_TIMEOUT_SECONDS)
            send_event(
                'presence_snapshot',
                'info',
                f'presence_snapshot:{SERVICE_NAME}',
                'Current active presence counts were refreshed.',
                snapshot,
            )
            state['last_presence_snapshot_unix'] = now
        if now - last_health >= MONITORING_SNAPSHOT_INTERVAL_SECONDS:
            snapshot = shared.get_health_snapshot(window_seconds=MONITORING_HEALTH_WINDOW_SECONDS)
            send_event(
                'health_snapshot',
                'info',
                f'health_snapshot:{SERVICE_NAME}',
                'Recent request health metrics were refreshed.',
                snapshot,
            )
            state['last_health_snapshot_unix'] = now
        _save_state(state)
