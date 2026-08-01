from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import sys
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Iterable
from urllib.parse import urlsplit

from fastapi import HTTPException, WebSocket
from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import RedisError


DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/0"
DEFAULT_PREFIX = "recroom:allapis:v1"
DEFAULT_MAX_CONNECTIONS = 64
DEFAULT_PRESENCE_TTL_SECONDS = 75
DEFAULT_HEARTBEAT_SECONDS = 25
DEFAULT_SERVERLESS_IDLE_SUSPEND_SECONDS = 60
DEFAULT_SERVERLESS_WAKE_TIMEOUT_SECONDS = 45
DEFAULT_LOCAL_WAKE_TIMEOUT_SECONDS = 5
DEFAULT_FANOUT_CONCURRENCY = 32
MAX_FANOUT_TARGETS = 5_000
MAX_FANOUT_MESSAGES = 8
MAX_FANOUT_MESSAGE_BYTES = 256 * 1024
MAX_PLAYER_SESSIONS = 128


class RedisConfigurationError(RuntimeError):
    pass


def _positive_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RedisConfigurationError(f"{name} must be an integer.") from exc
    if not minimum <= value <= maximum:
        raise RedisConfigurationError(
            f"{name} must be between {minimum} and {maximum}."
        )
    return value


def _nonnegative_int(name: str, default: int, *, maximum: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RedisConfigurationError(f"{name} must be an integer.") from exc
    if not 0 <= value <= maximum:
        raise RedisConfigurationError(f"{name} must be between 0 and {maximum}.")
    return value


def _redis_operation(method):
    @wraps(method)
    async def wrapped(self: "RedisTransientState", *args: Any, **kwargs: Any):
        async with self._operation():
            return await method(self, *args, **kwargs)

    return wrapped


def resolve_redis_url(*, production: bool) -> str:
    configured = str(os.getenv("REDIS_URL") or "").strip()
    if not configured:
        if production:
            raise RedisConfigurationError(
                "Railway/container mode requires REDIS_URL for shared transient state."
            )
        configured = DEFAULT_REDIS_URL
    parsed = urlsplit(configured)
    if parsed.scheme.casefold() not in {"redis", "rediss"} or not parsed.hostname:
        raise RedisConfigurationError("REDIS_URL must be a valid redis:// or rediss:// URL.")
    return configured


def safe_redis_endpoint(redis_url: str) -> str:
    parsed = urlsplit(redis_url)
    database = parsed.path.lstrip("/") or "0"
    port = parsed.port or (6380 if parsed.scheme.casefold() == "rediss" else 6379)
    return f"{parsed.scheme.casefold()}://{parsed.hostname}:{port}/{database}"


def resolve_prefix() -> str:
    raw = str(os.getenv("RECROOM_REDIS_PREFIX") or DEFAULT_PREFIX).strip()
    if not raw or len(raw) > 100:
        raise RedisConfigurationError("RECROOM_REDIS_PREFIX must contain 1 to 100 characters.")
    normalized = "".join(char if char.isalnum() or char in {"-", "_", ":"} else "_" for char in raw)
    return normalized.strip(":") or DEFAULT_PREFIX


class RedisTransientState:
    """Shared transient state and cross-process realtime delivery.

    Redis is authoritative for leases, presence, rate limits, short-lived
    sessions, routing metadata, and Pub/Sub coordination. ``_local_sockets``
    contains only the non-serializable WebSocket objects owned by this Python
    process; it is never consulted as authoritative online/shared state.
    """

    _RATE_LIMIT_SCRIPT = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return {current, ttl}
"""

    _GET_DELETE_SCRIPT = """
local value = redis.call('GET', KEYS[1])
if value then
  redis.call('DEL', KEYS[1])
end
return value
"""

    _LEASE_UPSERT_SCRIPT = """
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[5])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[6])
redis.call('ZADD', KEYS[4], ARGV[3], ARGV[6])
redis.call('ZADD', KEYS[5], ARGV[3], ARGV[4])
redis.call('EXPIRE', KEYS[5], ARGV[5])
if redis.call('EXISTS', KEYS[6]) == 1 then
  redis.call('EXPIRE', KEYS[6], ARGV[2])
end
return 1
"""

    _LEASE_REMOVE_SCRIPT = """
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[2])
local remaining = redis.call('ZCARD', KEYS[2])
redis.call('ZREM', KEYS[6], ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[6], '-inf', ARGV[2])
local route_remaining = redis.call('ZCARD', KEYS[6])
if route_remaining == 0 then
  redis.call('DEL', KEYS[6])
  redis.call('ZREM', KEYS[5], ARGV[3])
end
if remaining == 0 then
  redis.call('DEL', KEYS[2])
  redis.call('DEL', KEYS[3])
  redis.call('ZREM', KEYS[4], ARGV[3])
end
return {remaining, route_remaining}
"""

    _MEMBERSHIP_UPDATE_SCRIPT = """
local old_session = redis.call('GET', KEYS[2])
if old_session and old_session ~= ARGV[1] then
  redis.call('ZREM', ARGV[6] .. old_session, ARGV[4])
end
if ARGV[1] == '' then
  redis.call('DEL', KEYS[1])
  redis.call('DEL', KEYS[2])
  return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[5], ARGV[4])
redis.call('EXPIRE', KEYS[3], ARGV[3] * 3)
return 1
"""

    _VOTE_TO_KICK_SCRIPT = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[4])
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) or
   not redis.call('ZSCORE', KEYS[1], ARGV[2]) then
  return {-1, 0, 0, '', 0}
end
local started = 0
if ARGV[3] == '1' then
  redis.call('SADD', KEYS[2], ARGV[1])
  if not redis.call('GET', KEYS[5]) then
    redis.call('SET', KEYS[5], ARGV[1])
    started = 1
  end
else
  redis.call('SREM', KEYS[2], ARGV[1])
end
redis.call('EXPIRE', KEYS[2], ARGV[5])
redis.call('EXPIRE', KEYS[5], ARGV[5])
local members = redis.call('ZCARD', KEYS[1])
local eligible = members - 1
local required = math.max(1, math.floor(eligible / 2) + 1)
local votes = redis.call('SCARD', KEYS[2])
local initiator = redis.call('GET', KEYS[5]) or ''
if votes >= required then
  redis.call('ZREM', KEYS[1], ARGV[2])
  redis.call('DEL', KEYS[2])
  redis.call('DEL', KEYS[3])
  redis.call('DEL', KEYS[4])
  redis.call('DEL', KEYS[5])
  return {1, votes, required, initiator, started}
end
return {0, votes, required, initiator, started}
"""

    _REMOVE_SESSION_MEMBER_SCRIPT = """
local current = redis.call('GET', KEYS[2])
if current ~= ARGV[1] then
  redis.call('ZREM', KEYS[3], ARGV[2])
  return 0
end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[3], ARGV[2])
return 1
"""

    _PLAYER_SESSION_CREATE_SCRIPT = """
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[5])
local count = redis.call('ZCARD', KEYS[2])
local maximum = tonumber(ARGV[6])
if count > maximum then
  local stale = redis.call('ZRANGE', KEYS[2], 0, count - maximum - 1)
  for _, token_hash in ipairs(stale) do
    redis.call('DEL', ARGV[7] .. token_hash)
    redis.call('ZREM', KEYS[2], token_hash)
  end
end
local ttl = redis.call('TTL', KEYS[2])
if ttl < tonumber(ARGV[2]) then
  redis.call('EXPIRE', KEYS[2], ARGV[2])
end
return 1
"""

    def __init__(
        self,
        redis_url: str,
        *,
        prefix: str,
        production: bool,
        max_connections: int | None = None,
        presence_ttl_seconds: int | None = None,
        idle_suspend_seconds: int | None = None,
        wake_timeout_seconds: int | None = None,
    ) -> None:
        self.redis_url = redis_url
        self.prefix = prefix
        self.production = production
        self.max_connections = max_connections or _positive_int(
            "RECROOM_REDIS_MAX_CONNECTIONS",
            DEFAULT_MAX_CONNECTIONS,
            minimum=8,
            maximum=512,
        )
        self.presence_ttl_seconds = presence_ttl_seconds or _positive_int(
            "RECROOM_PRESENCE_TTL_SECONDS",
            DEFAULT_PRESENCE_TTL_SECONDS,
            minimum=30,
            maximum=300,
        )
        self.heartbeat_seconds = _positive_int(
            "RECROOM_PRESENCE_HEARTBEAT_SECONDS",
            DEFAULT_HEARTBEAT_SECONDS,
            minimum=5,
            maximum=max(5, self.presence_ttl_seconds - 5),
        )
        self.idle_suspend_seconds = (
            max(0, int(idle_suspend_seconds))
            if idle_suspend_seconds is not None
            else _nonnegative_int(
                "RECROOM_REDIS_IDLE_SUSPEND_SECONDS",
                DEFAULT_SERVERLESS_IDLE_SUSPEND_SECONDS if production else 0,
                maximum=3_600,
            )
        )
        self.wake_timeout_seconds = (
            max(1, int(wake_timeout_seconds))
            if wake_timeout_seconds is not None
            else _positive_int(
                "RECROOM_REDIS_WAKE_TIMEOUT_SECONDS",
                (
                    DEFAULT_SERVERLESS_WAKE_TIMEOUT_SECONDS
                    if production
                    else DEFAULT_LOCAL_WAKE_TIMEOUT_SECONDS
                ),
                minimum=1,
                maximum=180,
            )
        )
        self.instance_id = uuid.uuid4().hex
        self._pool = ConnectionPool.from_url(
            redis_url,
            max_connections=self.max_connections,
            decode_responses=True,
            socket_connect_timeout=3.0,
            socket_timeout=5.0,
            health_check_interval=0,
            retry_on_timeout=True,
        )
        self._redis = Redis(connection_pool=self._pool)
        self._started = False
        self._closed = False
        self._lifecycle_lock = asyncio.Lock()
        self._active_operations = 0
        self._last_activity_monotonic = time.monotonic()
        self._subscriber_task: asyncio.Task[None] | None = None
        self._lease_refresh_task: asyncio.Task[None] | None = None
        self._idle_suspend_task: asyncio.Task[None] | None = None
        self._local_sockets: dict[tuple[str, str, str], dict[str, WebSocket]] = defaultdict(dict)
        self._local_connection_route: dict[str, tuple[str, str, str]] = {}
        self.fanout_concurrency = _positive_int(
                "RECROOM_REDIS_FANOUT_CONCURRENCY",
                DEFAULT_FANOUT_CONCURRENCY,
                minimum=4,
                maximum=256,
        )
        self._fanout_semaphore = asyncio.Semaphore(self.fanout_concurrency)

    def _key(self, *parts: Any) -> str:
        cleaned = []
        for part in parts:
            value = str(part or "")
            if len(value) > 96 or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for char in value):
                value = hashlib.sha256(value.encode("utf-8")).hexdigest()
            cleaned.append(value)
        return ":".join((self.prefix, *cleaned))

    @staticmethod
    def _opaque(value: Any) -> str:
        return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()

    @property
    def redis(self) -> Redis:
        if not self._started:
            raise HTTPException(status_code=503, detail="Shared transient state is unavailable.")
        return self._redis

    async def start(self) -> None:
        self._closed = False
        try:
            await self.ensure_active()
        except HTTPException as exc:
            raise RedisConfigurationError(str(exc.detail)) from exc
        if self.idle_suspend_seconds and self._idle_suspend_task is None:
            self._idle_suspend_task = asyncio.create_task(
                self._idle_suspend_loop(), name="recroom-redis-idle-suspend"
            )

    async def ensure_active(self) -> None:
        self._last_activity_monotonic = time.monotonic()
        async with self._lifecycle_lock:
            if self._closed:
                raise HTTPException(
                    status_code=503,
                    detail="Shared transient state is shutting down.",
                )
            if self._started:
                return
            await self._activate_locked()

    async def _activate_locked(self) -> None:
        last_error: Exception | None = None
        deadline = time.monotonic() + self.wake_timeout_seconds
        attempt = 0
        while True:
            try:
                if await self._redis.ping() is not True:
                    raise RedisError("Redis PING did not return success.")
                self._started = True
                self._subscriber_task = asyncio.create_task(
                    self._subscriber_loop(), name="recroom-redis-realtime-subscriber"
                )
                self._lease_refresh_task = asyncio.create_task(
                    self._lease_refresh_loop(), name="recroom-redis-lease-refresh"
                )
                idle_suspend = (
                    f"{self.idle_suspend_seconds}s"
                    if self.idle_suspend_seconds
                    else "disabled"
                )
                print(
                    "Redis transient state connected: "
                    f"{safe_redis_endpoint(self.redis_url)}; prefix={self.prefix}; "
                    f"pool={self.max_connections}; presence_ttl={self.presence_ttl_seconds}s; "
                    f"heartbeat={self.heartbeat_seconds}s; "
                    f"idle_suspend={idle_suspend}",
                    file=sys.stderr,
                )
                return
            except (RedisError, OSError) as exc:
                last_error = exc
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                delay = min(2.0, 0.2 * (2**min(attempt, 4)))
                await asyncio.sleep(min(remaining, delay + secrets.randbelow(100) / 1000))
                attempt += 1
        raise self._unavailable(last_error or RedisError("Redis wake timed out."))

    @asynccontextmanager
    async def _operation(self):
        async with self._lifecycle_lock:
            if self._closed:
                raise HTTPException(
                    status_code=503,
                    detail="Shared transient state is shutting down.",
                )
            self._active_operations += 1
            self._last_activity_monotonic = time.monotonic()
        try:
            await self.ensure_active()
            yield
        finally:
            async with self._lifecycle_lock:
                self._active_operations = max(0, self._active_operations - 1)
                self._last_activity_monotonic = time.monotonic()

    async def _idle_suspend_loop(self) -> None:
        poll_seconds = min(5.0, max(0.1, self.idle_suspend_seconds / 4))
        while not self._closed:
            try:
                await asyncio.sleep(poll_seconds)
                await self.suspend_if_idle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(
                    f"Redis idle suspension check failed ({type(exc).__name__}).",
                    file=sys.stderr,
                )

    async def suspend_if_idle(self) -> bool:
        if not self.idle_suspend_seconds:
            return False
        async with self._lifecycle_lock:
            idle_for = time.monotonic() - self._last_activity_monotonic
            if (
                not self._started
                or self._active_operations
                or self.local_connection_count()
                or idle_for < self.idle_suspend_seconds
            ):
                return False
            await self._suspend_locked()
            print(
                "Redis transient state suspended after "
                f"{self.idle_suspend_seconds}s without local activity.",
                file=sys.stderr,
            )
            return True

    async def _suspend_locked(self) -> None:
        self._started = False
        tasks = tuple(
            task
            for task in (self._subscriber_task, self._lease_refresh_task)
            if task is not None
        )
        self._subscriber_task = None
        self._lease_refresh_task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self._pool.disconnect(inuse_connections=True)

    async def close(self) -> None:
        async with self._lifecycle_lock:
            self._closed = True
            self._started = False
            local_sockets = tuple(
                websocket
                for sockets in self._local_sockets.values()
                for websocket in sockets.values()
            )
            tasks = tuple(
                task
                for task in (
                    self._subscriber_task,
                    self._lease_refresh_task,
                    self._idle_suspend_task,
                )
                if task is not None
            )
            self._subscriber_task = None
            self._lease_refresh_task = None
            self._idle_suspend_task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if local_sockets:
            await asyncio.gather(
                *(
                    websocket.close(code=1012, reason="Server restarting.")
                    for websocket in local_sockets
                ),
                return_exceptions=True,
            )
        await self._redis.aclose(close_connection_pool=False)
        await self._pool.aclose()
        self._local_sockets.clear()
        self._local_connection_route.clear()

    def _unavailable(self, exc: Exception) -> HTTPException:
        return HTTPException(
            status_code=503,
            detail=f"Shared transient state is unavailable ({type(exc).__name__}).",
        )

    @_redis_operation
    async def allow_rate_limit(self, bucket: str, *, limit: int, window_seconds: int) -> bool:
        key = self._key("rate", self._opaque(bucket))
        try:
            result = await self.redis.eval(
                self._RATE_LIMIT_SCRIPT,
                1,
                key,
                max(1, window_seconds) * 1000,
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return int(result[0]) <= limit

    @_redis_operation
    async def put_json(self, family: str, identifier: Any, value: Any, *, ttl_seconds: int) -> None:
        try:
            await self.redis.set(
                self._key(family, identifier),
                json.dumps(value, separators=(",", ":"), ensure_ascii=False),
                ex=max(1, ttl_seconds),
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def get_json(self, family: str, identifier: Any) -> Any | None:
        try:
            value = await self.redis.get(self._key(family, identifier))
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        if value is None:
            return None
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return None

    @_redis_operation
    async def take_json(self, family: str, identifier: Any) -> Any | None:
        """Atomically consume a short-lived JSON value."""

        try:
            value = await self.redis.eval(
                self._GET_DELETE_SCRIPT,
                1,
                self._key(family, identifier),
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        if value is None:
            return None
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return None

    @_redis_operation
    async def delete(self, family: str, identifier: Any) -> None:
        try:
            await self.redis.delete(self._key(family, identifier))
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def get_or_create_secret(
        self,
        family: str,
        identifier: Any,
        *,
        ttl_seconds: int,
    ) -> str:
        """Return one process-independent random secret without racing writers."""

        key = self._key("secret", family, identifier)
        generated = secrets.token_urlsafe(32)
        try:
            created = await self.redis.set(
                key,
                generated,
                ex=max(1, ttl_seconds),
                nx=True,
            )
            value = generated if created else await self.redis.get(key)
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        if not value:
            raise HTTPException(
                status_code=503,
                detail="Shared transient signing state is unavailable.",
            )
        return str(value)

    @_redis_operation
    async def get_secret(self, family: str, identifier: Any) -> str | None:
        try:
            value = await self.redis.get(self._key("secret", family, identifier))
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return str(value) if value else None

    @_redis_operation
    async def delete_secret(self, family: str, identifier: Any) -> None:
        try:
            await self.redis.delete(self._key("secret", family, identifier))
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def create_admin_session(
        self,
        *,
        token_hash: str,
        session: dict[str, Any],
        ttl_seconds: int,
    ) -> None:
        await self.put_json("admin-session", token_hash, session, ttl_seconds=ttl_seconds)

    @_redis_operation
    async def get_admin_session(self, token_hash: str) -> dict[str, Any] | None:
        value = await self.get_json("admin-session", token_hash)
        return value if isinstance(value, dict) else None

    @_redis_operation
    async def revoke_admin_session(self, token_hash: str) -> None:
        await self.delete("admin-session", token_hash)

    @_redis_operation
    async def create_player_session(
        self,
        *,
        token_hash: str,
        player_id: str,
        session: dict[str, Any],
        ttl_seconds: int,
    ) -> None:
        session_key = self._key("player-session", token_hash)
        player_sessions_key = self._key("player-sessions", player_id)
        ttl = max(1, ttl_seconds)
        try:
            await self.redis.eval(
                self._PLAYER_SESSION_CREATE_SCRIPT,
                2,
                session_key,
                player_sessions_key,
                json.dumps(session, separators=(",", ":"), ensure_ascii=False),
                ttl,
                int(time.time()) + ttl,
                token_hash,
                int(time.time()),
                MAX_PLAYER_SESSIONS,
                self._key("player-session", ""),
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def revoke_player_session(self, token_hash: str) -> None:
        session = await self.get_json("player-session", token_hash)
        try:
            pipe = self.redis.pipeline(transaction=True)
            pipe.delete(self._key("player-session", token_hash))
            if isinstance(session, dict) and session.get("player_id"):
                pipe.zrem(
                    self._key("player-sessions", session["player_id"]), token_hash
                )
            await pipe.execute()
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def revoke_player_sessions(self, player_id: Any) -> None:
        index_key = self._key("player-sessions", player_id)
        try:
            token_hashes = await self.redis.zrange(
                index_key, 0, MAX_PLAYER_SESSIONS - 1
            )
            pipe = self.redis.pipeline(transaction=True)
            for token_hash in token_hashes:
                pipe.delete(self._key("player-session", token_hash))
            pipe.delete(index_key)
            await pipe.execute()
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def register_connection(
        self,
        *,
        api_version: str,
        transport: str,
        player_id: Any,
        websocket: WebSocket,
        presence: dict[str, Any] | None = None,
    ) -> str:
        connection_id = uuid.uuid4().hex
        route = (str(api_version), str(transport), str(player_id))
        self._local_sockets[route][connection_id] = websocket
        self._local_connection_route[connection_id] = route
        try:
            await self.refresh_connection(
                connection_id=connection_id,
                api_version=api_version,
                transport=transport,
                player_id=player_id,
                presence=presence,
            )
        except Exception:
            self._local_sockets[route].pop(connection_id, None)
            if not self._local_sockets[route]:
                self._local_sockets.pop(route, None)
            self._local_connection_route.pop(connection_id, None)
            raise
        return connection_id

    @_redis_operation
    async def refresh_connection(
        self,
        *,
        connection_id: str,
        api_version: str,
        transport: str,
        player_id: Any,
        presence: dict[str, Any] | None = None,
    ) -> None:
        now = int(time.time())
        expires_at = now + self.presence_ttl_seconds
        player = str(player_id)
        lease = {
            "connection_id": connection_id,
            "instance_id": self.instance_id,
            "api_version": str(api_version),
            "transport": str(transport),
            "player_id": player,
            "expires_at": expires_at,
        }
        lease_key = self._key("connection", connection_id)
        player_key = self._key("player-connections", player)
        all_players_key = self._key("online-players")
        route_players_key = self._key("route-players", api_version, transport)
        try:
            await self.redis.eval(
                self._LEASE_UPSERT_SCRIPT,
                6,
                lease_key,
                player_key,
                all_players_key,
                route_players_key,
                self._key(
                    "route-connections", api_version, transport, player
                ),
                self._key("presence", api_version, player),
                json.dumps(lease, separators=(",", ":")),
                self.presence_ttl_seconds,
                expires_at,
                connection_id,
                self.presence_ttl_seconds * 3,
                player,
            )
            if presence is not None:
                await self.redis.set(
                    self._key("presence", api_version, player),
                    json.dumps(presence, separators=(",", ":"), ensure_ascii=False),
                    ex=self.presence_ttl_seconds,
                )
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        membership = (
            None
            if presence is None
            else presence.get("GameSession") or presence.get("GameSessionId")
        )
        if membership not in (None, "", 0, "0"):
            await self.set_membership(player, membership)

    @_redis_operation
    async def refresh_local_connection_leases(self) -> None:
        routes = tuple(self._local_connection_route.items())

        async def refresh(
            connection_id: str,
            route: tuple[str, str, str],
        ) -> None:
            if self._local_connection_route.get(connection_id) != route:
                return
            api_version, transport, player = route
            try:
                await self.refresh_connection(
                    connection_id=connection_id,
                    api_version=api_version,
                    transport=transport,
                    player_id=player,
                )
            except HTTPException:
                return
            if self._local_connection_route.get(connection_id) != route:
                await self._remove_lease(
                    connection_id=connection_id,
                    api_version=api_version,
                    transport=transport,
                    player=player,
                )

        batch_size = self.fanout_concurrency
        for offset in range(0, len(routes), batch_size):
            await asyncio.gather(
                *(
                    refresh(connection_id, route)
                    for connection_id, route in routes[offset : offset + batch_size]
                )
            )

    async def _lease_refresh_loop(self) -> None:
        while self._started:
            try:
                await asyncio.sleep(self.heartbeat_seconds)
                if self._started:
                    await self.refresh_local_connection_leases()
            except asyncio.CancelledError:
                raise
            except (HTTPException, RedisError, OSError):
                continue

    @_redis_operation
    async def unregister_connection(self, connection_id: str) -> bool:
        route = self._local_connection_route.pop(connection_id, None)
        if route is None:
            return False
        api_version, transport, player = route
        sockets = self._local_sockets.get(route)
        if sockets is not None:
            sockets.pop(connection_id, None)
            if not sockets:
                self._local_sockets.pop(route, None)
        return await self._remove_lease(
            connection_id=connection_id,
            api_version=api_version,
            transport=transport,
            player=player,
        )

    async def _remove_lease(
        self,
        *,
        connection_id: str,
        api_version: str,
        transport: str,
        player: str,
    ) -> bool:
        try:
            remaining = await self.redis.eval(
                self._LEASE_REMOVE_SCRIPT,
                6,
                self._key("connection", connection_id),
                self._key("player-connections", player),
                self._key("presence", api_version, player),
                self._key("online-players"),
                self._key("route-players", api_version, transport),
                self._key(
                    "route-connections", api_version, transport, player
                ),
                connection_id,
                int(time.time()),
                player,
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return int(remaining[0]) > 0

    @_redis_operation
    async def update_http_presence(
        self,
        *,
        api_version: str,
        player_id: Any,
        presence: dict[str, Any],
        online: bool,
    ) -> None:
        player = str(player_id)
        connection_id = self._opaque(f"http-presence:{api_version}:{player}")
        if online:
            await self.refresh_connection(
                connection_id=connection_id,
                api_version=api_version,
                transport="http-presence",
                player_id=player,
                presence=presence,
            )
            return
        await self._remove_lease(
            connection_id=connection_id,
            api_version=api_version,
            transport="http-presence",
            player=player,
        )

    @_redis_operation
    async def set_presence(
        self,
        api_version: str,
        player_id: Any,
        presence: dict[str, Any],
        *,
        ttl_seconds: int | None = None,
    ) -> None:
        try:
            await self.redis.set(
                self._key("presence", api_version, player_id),
                json.dumps(presence, separators=(",", ":"), ensure_ascii=False),
                ex=ttl_seconds or self.presence_ttl_seconds,
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def get_presence(self, api_version: str, player_id: Any) -> dict[str, Any] | None:
        if not await self.player_online(player_id):
            return None
        try:
            value = await self.redis.get(self._key("presence", api_version, player_id))
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        if value is None:
            return None
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return None
        return decoded if isinstance(decoded, dict) else None

    @_redis_operation
    async def player_online(self, player_id: Any) -> bool:
        player = str(player_id)
        key = self._key("player-connections", player)
        now = int(time.time())
        try:
            pipe = self.redis.pipeline(transaction=True)
            pipe.zremrangebyscore(key, "-inf", now)
            pipe.zcard(key)
            result = await pipe.execute()
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return int(result[-1]) > 0

    @_redis_operation
    async def route_player_ids(self, api_version: str, transport: str) -> list[str]:
        key = self._key("route-players", api_version, transport)
        now = int(time.time())
        try:
            pipe = self.redis.pipeline(transaction=True)
            pipe.zremrangebyscore(key, "-inf", now)
            pipe.zrangebyscore(key, now + 1, "+inf", start=0, num=MAX_FANOUT_TARGETS)
            result = await pipe.execute()
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return [str(value) for value in result[-1]]

    @_redis_operation
    async def route_player_online(
        self, api_version: str, transport: str, player_id: Any
    ) -> bool:
        try:
            score = await self.redis.zscore(
                self._key("route-players", api_version, transport), str(player_id)
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return score is not None and float(score) > time.time()

    @_redis_operation
    async def set_membership(self, player_id: Any, membership: Any | None) -> None:
        session_id: str = ""
        if isinstance(membership, dict):
            session_id = str(
                membership.get("GameSessionId")
                or membership.get("gameSessionId")
                or membership.get("game_session_id")
                or ""
            )
        elif membership not in (None, "", 0, "0"):
            session_id = str(membership)
        if session_id in {"0", "None"}:
            session_id = ""
        player = str(player_id)
        roster_key = self._key("session-members", session_id)
        roster_prefix = self._key("session-members", "")
        try:
            await self.redis.eval(
                self._MEMBERSHIP_UPDATE_SCRIPT,
                3,
                self._key("membership", player),
                self._key("membership-session", player),
                roster_key,
                session_id,
                json.dumps(membership, separators=(",", ":"), ensure_ascii=False)
                if session_id
                else "null",
                self.presence_ttl_seconds,
                player,
                int(time.time()) + self.presence_ttl_seconds,
                roster_prefix,
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def get_membership(self, player_id: Any) -> Any | None:
        return await self.get_json("membership", player_id)

    @_redis_operation
    async def session_member_ids(self, game_session_id: Any) -> list[str]:
        key = self._key("session-members", game_session_id)
        now = int(time.time())
        try:
            pipe = self.redis.pipeline(transaction=True)
            pipe.zremrangebyscore(key, "-inf", now)
            pipe.zrangebyscore(key, now + 1, "+inf", start=0, num=MAX_FANOUT_TARGETS)
            result = await pipe.execute()
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return [str(value) for value in result[-1]]

    @_redis_operation
    async def session_member_count(self, game_session_id: Any) -> int:
        return len(await self.session_member_ids(game_session_id))

    @_redis_operation
    async def session_member_counts(
        self, game_session_ids: Iterable[Any]
    ) -> dict[str, int]:
        session_ids = list(
            dict.fromkeys(str(value) for value in game_session_ids if str(value))
        )[:MAX_FANOUT_TARGETS]
        now = int(time.time())
        try:
            cleanup = self.redis.pipeline(transaction=False)
            for session_id in session_ids:
                cleanup.zremrangebyscore(
                    self._key("session-members", session_id), "-inf", now
                )
            if session_ids:
                await cleanup.execute()
            counts = self.redis.pipeline(transaction=False)
            for session_id in session_ids:
                counts.zcard(self._key("session-members", session_id))
            values = await counts.execute() if session_ids else []
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return {
            session_id: int(value)
            for session_id, value in zip(session_ids, values)
        }

    @_redis_operation
    async def authorize_session_party(
        self,
        game_session_id: Any,
        host_id: Any,
        player_ids: Iterable[Any],
    ) -> None:
        party_key = self._key("session-party", game_session_id)
        host_key = self._key("session-host", game_session_id)
        values = list(
            dict.fromkeys(
                str(value)
                for value in (host_id, *player_ids)
                if str(value)
            )
        )[:256]
        ttl = self.presence_ttl_seconds * 12
        try:
            pipe = self.redis.pipeline(transaction=True)
            if values:
                pipe.sadd(party_key, *values)
            pipe.expire(party_key, ttl)
            pipe.set(host_key, str(host_id), ex=ttl, nx=True)
            await pipe.execute()
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def session_party_member_ids(self, game_session_id: Any) -> set[str]:
        try:
            values = await self.redis.smembers(
                self._key("session-party", game_session_id)
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return {str(value) for value in values}

    @_redis_operation
    async def session_host_id(self, game_session_id: Any) -> str | None:
        try:
            value = await self.redis.get(self._key("session-host", game_session_id))
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return str(value) if value else None

    @_redis_operation
    async def record_session_invites(
        self,
        game_session_id: Any,
        inviter_id: Any,
        player_ids: Iterable[Any],
    ) -> None:
        values = list(
            dict.fromkeys(str(value) for value in player_ids if str(value))
        )[:256]
        if not values:
            return
        key = self._key("session-invites", game_session_id)
        mapping = {value: str(inviter_id) for value in values}
        try:
            pipe = self.redis.pipeline(transaction=True)
            pipe.hset(key, mapping=mapping)
            pipe.expire(key, self.presence_ttl_seconds * 12)
            await pipe.execute()
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def session_inviter_id(
        self, game_session_id: Any, player_id: Any
    ) -> str | None:
        try:
            value = await self.redis.hget(
                self._key("session-invites", game_session_id), str(player_id)
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return str(value) if value else None

    @_redis_operation
    async def consume_session_invite(
        self, game_session_id: Any, player_id: Any
    ) -> str | None:
        key = self._key("session-invites", game_session_id)
        try:
            inviter = await self.redis.hget(key, str(player_id))
            if inviter is not None:
                await self.redis.hdel(key, str(player_id))
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return str(inviter) if inviter else None

    @_redis_operation
    async def remove_session_member(
        self, game_session_id: Any, player_id: Any
    ) -> bool:
        player = str(player_id)
        try:
            removed = await self.redis.eval(
                self._REMOVE_SESSION_MEMBER_SCRIPT,
                3,
                self._key("membership", player),
                self._key("membership-session", player),
                self._key("session-members", game_session_id),
                str(game_session_id),
                player,
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        return bool(removed)

    @_redis_operation
    async def vote_to_kick(
        self,
        *,
        game_session_id: Any,
        voter_id: Any,
        target_id: Any,
        vote_yes: bool,
    ) -> tuple[str, int, int, int, bool]:
        try:
            result = await self.redis.eval(
                self._VOTE_TO_KICK_SCRIPT,
                5,
                self._key("session-members", game_session_id),
                self._key("votekick", game_session_id, target_id),
                self._key("membership", target_id),
                self._key("membership-session", target_id),
                self._key("votekick-initiator", game_session_id, target_id),
                str(voter_id),
                str(target_id),
                "1" if vote_yes else "0",
                int(time.time()),
                self.presence_ttl_seconds,
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc
        status = int(result[0])
        return (
            "not_members" if status < 0 else "kicked" if status > 0 else "recorded",
            int(result[1]),
            int(result[2]),
            int(result[3] or 0),
            bool(int(result[4])),
        )

    @_redis_operation
    async def remember_token_lookup(
        self, family: str, raw_token: str, value: Any, *, ttl_seconds: int
    ) -> None:
        await self.put_json(f"token:{family}", self._opaque(raw_token), value, ttl_seconds=ttl_seconds)

    @_redis_operation
    async def token_lookup(self, family: str, raw_token: str) -> Any | None:
        return await self.get_json(f"token:{family}", self._opaque(raw_token))

    @_redis_operation
    async def forget_token_lookup(self, family: str, raw_token: str) -> None:
        await self.delete(f"token:{family}", self._opaque(raw_token))

    @_redis_operation
    async def revoke_player_transient_state(
        self,
        player_id: Any,
        *,
        aliases: Iterable[Any] = (),
    ) -> None:
        players = list(
            dict.fromkeys(
                str(value)
                for value in (player_id, *aliases)
                if str(value)
            )
        )
        try:
            pipe = self.redis.pipeline(transaction=True)
            for player in players:
                session_id = await self.redis.get(
                    self._key("membership-session", player)
                )
                connection_ids = await self.redis.zrange(
                    self._key("player-connections", player), 0, -1
                )
                for connection_id in connection_ids[:256]:
                    raw_lease = await self.redis.get(
                        self._key("connection", connection_id)
                    )
                    try:
                        lease = json.loads(raw_lease or "{}")
                    except (TypeError, ValueError):
                        lease = {}
                    pipe.delete(self._key("connection", connection_id))
                    if isinstance(lease, dict):
                        api_version = str(lease.get("api_version") or "")
                        transport = str(lease.get("transport") or "")
                        if api_version:
                            pipe.delete(self._key("presence", api_version, player))
                        if api_version and transport:
                            pipe.delete(
                                self._key(
                                    "route-connections",
                                    api_version,
                                    transport,
                                    player,
                                )
                            )
                            pipe.zrem(
                                self._key(
                                    "route-players", api_version, transport
                                ),
                                player,
                            )
                pipe.delete(self._key("player-connections", player))
                pipe.delete(self._key("membership", player))
                pipe.delete(self._key("membership-session", player))
                if session_id:
                    pipe.zrem(self._key("session-members", session_id), player)
                pipe.zrem(self._key("online-players"), player)
            await pipe.execute()
            await self.revoke_player_sessions(player_id)
            await self.publish_delivery(
                api_version="*",
                transport="*",
                player_ids=players,
                messages=[],
                disconnect_code=1008,
                disconnect_reason="Session revoked.",
            )
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    @_redis_operation
    async def publish_delivery(
        self,
        *,
        api_version: str,
        transport: str,
        player_ids: Iterable[Any],
        messages: Iterable[str],
        disconnect_code: int | None = None,
        disconnect_reason: str = "",
    ) -> None:
        targets = list(dict.fromkeys(str(value) for value in player_ids if str(value)))[:MAX_FANOUT_TARGETS]
        encoded_messages = [str(message) for message in messages][:MAX_FANOUT_MESSAGES]
        if sum(len(message.encode("utf-8")) for message in encoded_messages) > MAX_FANOUT_MESSAGE_BYTES:
            raise HTTPException(status_code=413, detail="Realtime fan-out payload is too large.")
        envelope = {
            "event_id": uuid.uuid4().hex,
            "source_instance_id": self.instance_id,
            "api_version": str(api_version),
            "transport": str(transport),
            "player_ids": targets,
            "messages": encoded_messages,
            "disconnect_code": disconnect_code,
            "disconnect_reason": str(disconnect_reason)[:120],
        }
        try:
            await self.redis.publish(
                self._key("realtime"),
                json.dumps(envelope, separators=(",", ":"), ensure_ascii=False),
            )
            await self._deliver_local(envelope)
        except RedisError as exc:
            raise self._unavailable(exc) from exc

    def local_connection_count(
        self,
        *,
        api_version: str | None = None,
        transport: str | None = None,
    ) -> int:
        return sum(
            len(sockets)
            for (route_api, route_transport, _), sockets in self._local_sockets.items()
            if (api_version is None or route_api == api_version)
            and (transport is None or route_transport == transport)
        )

    async def _subscriber_loop(self) -> None:
        delay = 0.2
        while self._started:
            pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
            try:
                await pubsub.subscribe(self._key("realtime"))
                delay = 0.2
                while self._started:
                    message = await pubsub.get_message(timeout=1.0)
                    if not message:
                        continue
                    try:
                        envelope = json.loads(message.get("data") or "{}")
                    except (TypeError, ValueError):
                        continue
                    if (
                        isinstance(envelope, dict)
                        and str(envelope.get("source_instance_id") or "")
                        != self.instance_id
                    ):
                        await self._deliver_local(envelope)
            except asyncio.CancelledError:
                raise
            except (RedisError, OSError):
                if self._started:
                    await asyncio.sleep(delay + secrets.randbelow(100) / 1000)
                    delay = min(5.0, delay * 2)
            finally:
                await pubsub.aclose()

    async def _deliver_local(self, envelope: dict[str, Any]) -> None:
        api_version = str(envelope.get("api_version") or "")
        transport = str(envelope.get("transport") or "")
        player_ids = {
            str(value) for value in list(envelope.get("player_ids") or [])[:MAX_FANOUT_TARGETS]
        }
        messages = [str(value) for value in list(envelope.get("messages") or [])[:MAX_FANOUT_MESSAGES]]
        disconnect_code = envelope.get("disconnect_code")
        disconnect_reason = str(envelope.get("disconnect_reason") or "")[:120]
        deliveries: list[tuple[str, WebSocket]] = []
        for (route_api, route_transport, player_id), sockets in tuple(self._local_sockets.items()):
            if api_version not in {"*", route_api} or transport not in {"*", route_transport}:
                continue
            if player_ids and player_id not in player_ids:
                continue
            deliveries.extend(tuple(sockets.items()))

        async def send(connection_id: str, websocket: WebSocket) -> None:
            async with self._fanout_semaphore:
                failed = False
                try:
                    for payload in messages:
                        await websocket.send_text(payload)
                    if disconnect_code is not None:
                        await websocket.close(code=int(disconnect_code), reason=disconnect_reason)
                except Exception:
                    failed = True
                finally:
                    if failed or disconnect_code is not None:
                        try:
                            await self.unregister_connection(connection_id)
                        except Exception:
                            pass

        batch_size = self.fanout_concurrency
        for offset in range(0, len(deliveries), batch_size):
            await asyncio.gather(
                *(send(connection_id, websocket) for connection_id, websocket in deliveries[offset : offset + batch_size]),
                return_exceptions=True,
            )


def build_transient_state(*, production: bool) -> RedisTransientState:
    return RedisTransientState(
        resolve_redis_url(production=production),
        prefix=resolve_prefix(),
        production=production,
    )
