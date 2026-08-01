from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable


def utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Canonical schedule timestamps must be timezone-aware.")
    return parsed.astimezone(timezone.utc)


def stable_period_id(schedule_key: str, revision: int, period_index: int) -> str:
    digest = hashlib.sha256(
        f"{schedule_key}|{int(revision)}|{int(period_index)}".encode("utf-8")
    ).hexdigest()[:24]
    return f"{schedule_key}:{int(revision)}:{int(period_index)}:{digest}"


def ensure_anchored_schedule(
    db: Any,
    *,
    schedule_key: str,
    anchor_utc: datetime,
    interval_seconds: int,
    catalog_revision: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if anchor_utc.tzinfo is None:
        raise ValueError("Schedule anchor must be timezone-aware.")
    if interval_seconds <= 0:
        raise ValueError("Schedule interval must be positive.")
    now_text = utc_text(datetime.now(timezone.utc))
    normalized_metadata = json.loads(
        json.dumps(metadata or {}, sort_keys=True, separators=(",", ":"))
    )
    config = {
        "anchor_utc": utc_text(anchor_utc),
        "interval_seconds": int(interval_seconds),
        "metadata": normalized_metadata,
    }
    with db.transaction() as conn:
        row = conn.execute(
            "SELECT * FROM timed_content_schedules WHERE schedule_key = ?",
            (schedule_key,),
        ).fetchone()
        if row is None:
            conn.execute(
                """
                INSERT INTO timed_content_schedules(
                    schedule_key, model, revision, catalog_revision, config_json,
                    active, created_at, updated_at
                )
                VALUES (?, 'anchored_interval', 1, ?, ?, 1, ?, ?)
                """,
                (
                    schedule_key,
                    str(catalog_revision),
                    json.dumps(config, sort_keys=True),
                    now_text,
                    now_text,
                ),
            )
        else:
            existing_config = json.loads(row["config_json"])
            if (
                existing_config.get("anchor_utc") != config["anchor_utc"]
                or int(existing_config.get("interval_seconds", 0)) != int(interval_seconds)
            ):
                raise ValueError(
                    f"Canonical schedule {schedule_key!r} already exists with an incompatible anchor or interval."
                )
            if (
                str(row["catalog_revision"]) != str(catalog_revision)
                or existing_config.get("metadata") != config["metadata"]
            ):
                # Existing period rows are immutable snapshots. Updating the
                # pending catalog here affects only periods not materialized
                # yet, including the next boundary after an operator change.
                conn.execute(
                    """
                    UPDATE timed_content_schedules
                    SET catalog_revision = ?, config_json = ?, updated_at = ?
                    WHERE schedule_key = ?
                    """,
                    (
                        str(catalog_revision),
                        json.dumps(config, sort_keys=True),
                        now_text,
                        schedule_key,
                    ),
                )
        row = conn.execute(
            "SELECT * FROM timed_content_schedules WHERE schedule_key = ?",
            (schedule_key,),
        ).fetchone()
    return dict(row)


def reconcile_anchored_period(
    db: Any,
    *,
    schedule_key: str,
    now_utc: datetime,
    materialize: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    if now_utc.tzinfo is None:
        raise ValueError("Reconciliation time must be timezone-aware.")
    now_utc = now_utc.astimezone(timezone.utc)
    with db.connection() as conn:
        schedule_row = conn.execute(
            "SELECT * FROM timed_content_schedules WHERE schedule_key = ? AND active = 1",
            (schedule_key,),
        ).fetchone()
    if schedule_row is None:
        raise KeyError(schedule_key)
    schedule = dict(schedule_row)
    config = json.loads(schedule["config_json"])
    anchor = parse_utc(config["anchor_utc"])
    interval_seconds = int(config["interval_seconds"])
    elapsed_seconds = (now_utc - anchor).total_seconds()
    period_index = int(elapsed_seconds // interval_seconds)
    period_start = anchor.timestamp() + period_index * interval_seconds
    starts_at = datetime.fromtimestamp(period_start, tz=timezone.utc)
    ends_at = datetime.fromtimestamp(period_start + interval_seconds, tz=timezone.utc)
    period_id = stable_period_id(schedule_key, int(schedule["revision"]), period_index)

    with db.connection() as conn:
        existing = conn.execute(
            """
            SELECT *
            FROM timed_content_periods
            WHERE schedule_key = ? AND period_id = ?
            """,
            (schedule_key, period_id),
        ).fetchone()
    if existing is not None:
        result = dict(existing)
        result["content"] = json.loads(result.pop("content_json"))
        return result

    materialization_input = {
        "schedule_key": schedule_key,
        "schedule_revision": int(schedule["revision"]),
        "catalog_revision": str(schedule["catalog_revision"]),
        "period_index": period_index,
        "period_id": period_id,
        "starts_at_utc": utc_text(starts_at),
        "ends_at_utc": utc_text(ends_at),
    }
    content = materialize(dict(materialization_input))
    if not isinstance(content, dict):
        raise TypeError("Timed-content materializer must return a JSON object.")
    content_json = json.dumps(content, sort_keys=True, separators=(",", ":"))
    materialized_at = utc_text(now_utc)
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO timed_content_periods(
                schedule_key, period_id, schedule_revision, catalog_revision,
                period_index, starts_at_utc, ends_at_utc, content_json,
                materialized_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                schedule_key,
                period_id,
                int(schedule["revision"]),
                str(schedule["catalog_revision"]),
                period_index,
                utc_text(starts_at),
                utc_text(ends_at),
                content_json,
                materialized_at,
            ),
        )
        winner = conn.execute(
            """
            SELECT *
            FROM timed_content_periods
            WHERE schedule_key = ? AND period_id = ?
            """,
            (schedule_key, period_id),
        ).fetchone()
    result = dict(winner)
    result["content"] = json.loads(result.pop("content_json"))
    return result


def _registered_materializer(
    schedule: dict[str, Any],
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    config = json.loads(schedule["config_json"])
    metadata = config.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("Timed-content schedule metadata must be an object.")
    strategy = str(metadata.get("strategy") or "")
    if strategy == "deterministic_sequence":
        sequence = metadata.get("sequence")
        if not isinstance(sequence, list) or not sequence:
            raise ValueError("Timed-content deterministic sequence metadata is invalid.")

        def materialize_sequence(period: dict[str, Any]) -> dict[str, Any]:
            period_index = int(period["period_index"])
            selected = sequence[period_index % len(sequence)]
            if not isinstance(selected, dict):
                raise ValueError("Timed-content sequence entries must be objects.")
            return dict(selected)

        return materialize_sequence
    if strategy == "rotating_window_mapping":
        base = metadata.get("base")
        mapping_key = str(metadata.get("mapping_key") or "")
        entry_key = str(metadata.get("entry_key") or "")
        candidates = metadata.get("candidates")
        window_size = int(metadata.get("window_size", 0) or 0)
        offset_bias = int(metadata.get("offset_bias", 0) or 0)
        if (
            not isinstance(base, dict)
            or not mapping_key
            or not entry_key
            or not isinstance(base.get(mapping_key), dict)
            or not isinstance(candidates, list)
            or not candidates
            or not all(isinstance(item, dict) for item in candidates)
            or window_size < 1
            or window_size > len(candidates)
        ):
            raise ValueError("Timed-content rotating-window metadata is invalid.")

        def materialize_rotating_window(period: dict[str, Any]) -> dict[str, Any]:
            content = json.loads(json.dumps(base, sort_keys=True))
            period_index = int(period["period_index"])
            offset = (offset_bias + period_index) % len(candidates)
            doubled = [*candidates, *candidates]
            content[mapping_key][entry_key] = [
                dict(item) for item in doubled[offset : offset + window_size]
            ]
            return content

        return materialize_rotating_window
    if strategy != "deterministic_pool_slots":
        raise ValueError(
            f"Timed-content schedule {schedule['schedule_key']!r} has no registered materializer."
        )
    pool = metadata.get("pool")
    reward_catalog = metadata.get("reward_catalog")
    slot_count = int(metadata.get("slot_count", 0) or 0)
    if (
        not isinstance(pool, list)
        or len(pool) < 2
        or not all(isinstance(item, dict) for item in pool)
        or not isinstance(reward_catalog, list)
        or not reward_catalog
        or not all(isinstance(item, dict) for item in reward_catalog)
        or slot_count < 1
        or slot_count > len(pool)
    ):
        raise ValueError("Timed-content deterministic pool metadata is invalid.")

    def materialize(period: dict[str, Any]) -> dict[str, Any]:
        starts_at = parse_utc(period["starts_at_utc"])
        iso = starts_at.isocalendar()
        map_id = (iso.year * 100) + iso.week
        pool_size = len(pool)
        start = map_id % pool_size
        step = 1 + ((map_id // pool_size) % (pool_size - 1))
        selected = [
            dict(pool[(start + (slot * step)) % pool_size])
            for slot in range(slot_count)
        ]
        reward = dict(reward_catalog[map_id % len(reward_catalog)])
        return {
            "map_id": map_id,
            "iso_year": iso.year,
            "iso_week": iso.week,
            "slots": selected,
            "reward": reward,
        }

    return materialize


def reconcile_registered_period(
    db: Any,
    *,
    schedule_key: str,
    now_utc: datetime,
) -> dict[str, Any]:
    with db.connection() as conn:
        row = conn.execute(
            "SELECT * FROM timed_content_schedules WHERE schedule_key = ? AND active = 1",
            (schedule_key,),
        ).fetchone()
    if row is None:
        raise KeyError(schedule_key)
    schedule = dict(row)
    return reconcile_anchored_period(
        db,
        schedule_key=schedule_key,
        now_utc=now_utc,
        materialize=_registered_materializer(schedule),
    )


def reconcile_due_timed_content(db: Any, *, now_utc: datetime) -> list[dict[str, Any]]:
    """Materialize every registered current period during process startup."""
    if now_utc.tzinfo is None:
        raise ValueError("Reconciliation time must be timezone-aware.")
    with db.connection() as conn:
        rows = conn.execute(
            "SELECT * FROM timed_content_schedules WHERE active = 1"
        ).fetchall()
    due: list[dict[str, Any]] = []
    for row in rows:
        schedule = dict(row)
        if schedule["model"] != "anchored_interval":
            continue
        config = json.loads(schedule["config_json"])
        anchor = parse_utc(config["anchor_utc"])
        interval_seconds = int(config["interval_seconds"])
        period_index = int((now_utc.astimezone(timezone.utc) - anchor).total_seconds() // interval_seconds)
        descriptor = {
            "schedule_key": schedule["schedule_key"],
            "period_id": stable_period_id(
                schedule["schedule_key"], int(schedule["revision"]), period_index
            ),
            "period_index": period_index,
            "materialized": False,
        }
        try:
            period = reconcile_registered_period(
                db,
                schedule_key=str(schedule["schedule_key"]),
                now_utc=now_utc,
            )
        except ValueError:
            # Unknown strategies remain inert rather than being guessed.
            descriptor["status"] = "unregistered_materializer"
        else:
            descriptor["materialized"] = True
            descriptor["status"] = "ready"
            descriptor["period_id"] = period["period_id"]
        due.append(descriptor)
    return due
