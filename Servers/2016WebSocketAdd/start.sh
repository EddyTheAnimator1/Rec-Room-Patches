#!/usr/bin/env bash
set -euo pipefail

export PYTHONUNBUFFERED=1
export WS_PORT="${WS_PORT:-8765}"

python socket_server.py &
WS_PID=$!

cleanup() {
  if kill -0 "$WS_PID" >/dev/null 2>&1; then
    kill "$WS_PID" || true
    wait "$WS_PID" || true
  fi
}
trap cleanup EXIT INT TERM

exec gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 4 --timeout 120 main:app
