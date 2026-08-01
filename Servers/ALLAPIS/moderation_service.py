from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any


OPEN_CASE_STATES = {
    "observed",
    "restricted",
    "timed_out",
    "quarantined",
    "banned",
    "review_required",
}
TARGET_TYPES = {"player", "room", "invention", "player_event", "image", "chat", "message"}
MIN_TIMEOUT_SECONDS = 1
MAX_TIMEOUT_SECONDS = 10 * 365 * 24 * 60 * 60


def utc_now_datetime() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: datetime | None = None) -> str:
    value = value or utc_now_datetime()
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _row_dict(row: Any | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def create_report(
    db: Any,
    *,
    reporter_player_id: str,
    target_type: str,
    target_id: str,
    canonical_category: str,
    raw_category: Any,
    category_schema: str,
    public_details: str,
    raw_details: str | None,
    room_id: str | None,
    game_session_id: str | None,
    source_version: str,
    source_endpoint: str,
    source_schema: str,
    source_payload: dict[str, Any] | None = None,
    evidence_status: str = "unavailable",
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or utc_now_datetime()
    created_at = utc_text(now)
    target_type = str(target_type).strip()
    target_id = str(target_id).strip()
    if target_type not in TARGET_TYPES:
        raise ValueError("Unsupported moderation target type.")
    if not reporter_player_id or not target_id:
        raise ValueError("Reporter and target are required.")
    canonical_category = str(canonical_category or "unknown").strip() or "unknown"
    category_schema = str(category_schema).strip()
    source_version = str(source_version).strip()
    source_endpoint = str(source_endpoint).strip()
    source_schema = str(source_schema).strip()
    public_details = str(public_details or "")[:20_000]
    raw_details = str(raw_details)[:20_000] if raw_details is not None else None
    report_id = str(uuid.uuid4())
    cutoff = utc_text(now - timedelta(hours=1))
    case_cutoff = utc_text(now - timedelta(hours=24))

    with db.transaction() as conn:
        cluster_identity = conn.execute(
            """
            SELECT identity_hash
            FROM player_identities
            WHERE player_id = ? AND identity_type = 'ip_hash'
            ORDER BY last_seen_at DESC
            LIMIT 1
            """,
            (reporter_player_id,),
        ).fetchone()
        reporter_cluster_id = (
            "network:"
            + hashlib.sha256(
                str(cluster_identity["identity_hash"]).encode("utf-8")
            ).hexdigest()[:24]
            if cluster_identity is not None
            else None
        )
        duplicate = conn.execute(
            """
            SELECT report_id
            FROM moderation_reports
            WHERE reporter_player_id = ?
              AND target_type = ?
              AND target_id = ?
              AND canonical_category = ?
              AND COALESCE(room_id, '') = COALESCE(?, '')
              AND COALESCE(game_session_id, '') = COALESCE(?, '')
              AND created_at >= ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                reporter_player_id,
                target_type,
                target_id,
                canonical_category,
                room_id,
                game_session_id,
                cutoff,
            ),
        ).fetchone()
        if duplicate is None and reporter_cluster_id is not None:
            # Shared-network reports remain valid allegations, but connected
            # accounts do not receive full independent weight for repeating
            # the same allegation in the same context.
            duplicate = conn.execute(
                """
                SELECT report_id
                FROM moderation_reports
                WHERE reporter_cluster_id = ?
                  AND target_type = ?
                  AND target_id = ?
                  AND canonical_category = ?
                  AND COALESCE(room_id, '') = COALESCE(?, '')
                  AND COALESCE(game_session_id, '') = COALESCE(?, '')
                  AND created_at >= ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (
                    reporter_cluster_id,
                    target_type,
                    target_id,
                    canonical_category,
                    room_id,
                    game_session_id,
                    cutoff,
                ),
            ).fetchone()
        duplicate_of = str(duplicate["report_id"]) if duplicate is not None else None
        counts_toward_case_score = 0 if duplicate_of else 1

        case_row = conn.execute(
            """
            SELECT mc.*
            FROM moderation_cases AS mc
            WHERE mc.target_type = ?
              AND mc.target_id = ?
              AND mc.canonical_category = ?
              AND mc.state IN (
                  'observed', 'restricted', 'timed_out', 'quarantined',
                  'banned', 'review_required'
              )
              AND mc.updated_at >= ?
            ORDER BY mc.updated_at DESC
            LIMIT 1
            """,
            (target_type, target_id, canonical_category, case_cutoff),
        ).fetchone()
        if case_row is None:
            case_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO moderation_cases(
                    case_id, target_type, target_id, canonical_category, state,
                    report_count, counting_report_count, assigned_to,
                    created_at, updated_at, closed_at
                )
                VALUES (?, ?, ?, ?, 'observed', 0, 0, NULL, ?, ?, NULL)
                """,
                (case_id, target_type, target_id, canonical_category, created_at, created_at),
            )
        else:
            case_id = str(case_row["case_id"])

        evidence_id: str | None = None
        if raw_details is not None and raw_details != public_details:
            evidence_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO moderation_evidence(
                    evidence_id, case_id, report_id, evidence_type, restricted,
                    public_text, raw_text, sha256, metadata_json,
                    created_at, retention_until, deleted_at
                )
                VALUES (?, ?, ?, 'report_details', 1, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    evidence_id,
                    case_id,
                    report_id,
                    public_details,
                    raw_details,
                    hashlib.sha256(raw_details.encode("utf-8")).hexdigest(),
                    _json({"source_version": source_version, "source_endpoint": source_endpoint}),
                    created_at,
                    utc_text(now + timedelta(days=180)),
                ),
            )
            evidence_status = "restricted"

        conn.execute(
            """
            INSERT INTO moderation_reports(
                report_id, case_id, reporter_player_id, target_type, target_id,
                canonical_category, raw_category_json, category_schema,
                public_details, protected_evidence_id, room_id, game_session_id,
                source_version, source_endpoint, source_schema, source_payload_json,
                client_request_id, duplicate_of, reporter_cluster_id,
                counts_toward_case_score, evidence_status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
            """,
            (
                report_id,
                case_id,
                reporter_player_id,
                target_type,
                target_id,
                canonical_category,
                _json(raw_category),
                category_schema,
                public_details,
                evidence_id,
                room_id,
                game_session_id,
                source_version,
                source_endpoint,
                source_schema,
                _json(source_payload or {}),
                duplicate_of,
                reporter_cluster_id,
                counts_toward_case_score,
                evidence_status,
                created_at,
            ),
        )
        conn.execute(
            """
            UPDATE moderation_cases
            SET report_count = report_count + 1,
                counting_report_count = counting_report_count + ?,
                updated_at = ?
            WHERE case_id = ?
            """,
            (counts_toward_case_score, created_at, case_id),
        )
        conn.execute(
            """
            INSERT INTO moderation_actions(
                action_id, case_id, target_type, target_id, actor_type, actor_id,
                action, previous_state, new_state, reason, duration_seconds,
                idempotency_key, reverses_action_id, metadata_json, created_at
            )
            VALUES (?, ?, ?, ?, 'player', ?, 'report_received', NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                case_id,
                target_type,
                target_id,
                reporter_player_id,
                _json(
                    {
                        "report_id": report_id,
                        "duplicate_of": duplicate_of,
                        "counts_toward_case_score": bool(counts_toward_case_score),
                    }
                ),
                created_at,
            ),
        )
    return {
        "report_id": report_id,
        "case_id": case_id,
        "duplicate_of": duplicate_of,
        "counts_toward_case_score": bool(counts_toward_case_score),
        "evidence_status": evidence_status,
    }


def list_cases(
    db: Any,
    *,
    state: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))
    params: list[Any] = []
    where = ""
    if state:
        where = "WHERE state = ?"
        params.append(str(state))
    params.extend([limit, offset])
    with db.connection() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM moderation_cases
            {where}
            ORDER BY
                CASE state WHEN 'review_required' THEN 0 WHEN 'banned' THEN 1
                     WHEN 'quarantined' THEN 2 WHEN 'restricted' THEN 3
                     WHEN 'timed_out' THEN 4 WHEN 'observed' THEN 5 ELSE 6 END,
                updated_at DESC
            LIMIT ? OFFSET ?
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_case(db: Any, case_id: str) -> dict[str, Any] | None:
    with db.connection() as conn:
        case = conn.execute(
            "SELECT * FROM moderation_cases WHERE case_id = ?",
            (case_id,),
        ).fetchone()
        if case is None:
            return None
        reports = conn.execute(
            """
            SELECT report_id, case_id, reporter_player_id, target_type, target_id,
                   canonical_category, raw_category_json, category_schema,
                   public_details, room_id, game_session_id, source_version,
                   source_endpoint, source_schema, duplicate_of,
                   reporter_cluster_id, counts_toward_case_score,
                   evidence_status, created_at
            FROM moderation_reports
            WHERE case_id = ?
            ORDER BY created_at
            """,
            (case_id,),
        ).fetchall()
        actions = conn.execute(
            """
            SELECT *
            FROM moderation_actions
            WHERE case_id = ?
            ORDER BY created_at
            """,
            (case_id,),
        ).fetchall()
        sanctions = conn.execute(
            """
            SELECT *
            FROM moderation_sanctions
            WHERE case_id = ?
            ORDER BY created_at
            """,
            (case_id,),
        ).fetchall()
        content_controls = conn.execute(
            """
            SELECT *
            FROM moderation_content_controls
            WHERE case_id = ?
            ORDER BY created_at
            """,
            (case_id,),
        ).fetchall()
    result = dict(case)
    result["reports"] = [dict(row) for row in reports]
    result["actions"] = [dict(row) for row in actions]
    result["sanctions"] = [dict(row) for row in sanctions]
    result["content_controls"] = [dict(row) for row in content_controls]
    return result


def get_case_evidence(db: Any, case_id: str, *, include_raw: bool) -> list[dict[str, Any]]:
    raw_column = "raw_text" if include_raw else "NULL AS raw_text"
    with db.connection() as conn:
        rows = conn.execute(
            f"""
            SELECT evidence_id, case_id, report_id, evidence_type, restricted,
                   public_text, {raw_column}, sha256, metadata_json,
                   created_at, retention_until, deleted_at
            FROM moderation_evidence
            WHERE case_id = ? AND deleted_at IS NULL
            ORDER BY created_at
            """,
            (case_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def append_operator_action(
    db: Any,
    *,
    case_id: str | None,
    target_type: str,
    target_id: str,
    actor_id: str,
    action: str,
    previous_state: str | None,
    new_state: str | None,
    reason: str,
    duration_seconds: int | None = None,
    idempotency_key: str | None = None,
    reverses_action_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> str:
    created_at = utc_text(now)
    action_id = str(uuid.uuid4())
    with db.transaction() as conn:
        if idempotency_key:
            existing = conn.execute(
                "SELECT action_id FROM moderation_actions WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                return str(existing["action_id"])
        conn.execute(
            """
            INSERT INTO moderation_actions(
                action_id, case_id, target_type, target_id, actor_type, actor_id,
                action, previous_state, new_state, reason, duration_seconds,
                idempotency_key, reverses_action_id, metadata_json, created_at
            )
            VALUES (?, ?, ?, ?, 'operator', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                action_id,
                case_id,
                target_type,
                target_id,
                actor_id,
                action,
                previous_state,
                new_state,
                reason,
                duration_seconds,
                idempotency_key,
                reverses_action_id,
                _json(metadata or {}),
                created_at,
            ),
        )
    return action_id


def record_evidence_access(
    db: Any,
    *,
    case_id: str,
    actor_id: str,
    evidence_ids: list[str],
    now: datetime | None = None,
) -> str:
    case = get_case(db, case_id)
    if case is None:
        raise KeyError(case_id)
    return append_operator_action(
        db,
        case_id=case_id,
        target_type=str(case["target_type"]),
        target_id=str(case["target_id"]),
        actor_id=actor_id,
        action="evidence_revealed",
        previous_state=str(case["state"]),
        new_state=str(case["state"]),
        reason="Authorized evidence review.",
        metadata={"evidence_ids": evidence_ids},
        now=now,
    )


def assign_case(
    db: Any,
    *,
    case_id: str,
    actor_id: str,
    assigned_to: str | None,
    reason: str,
    idempotency_key: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not reason.strip():
        raise ValueError("A moderator reason is required.")
    normalized_assignee = str(assigned_to).strip()[:200] if assigned_to is not None else None
    if normalized_assignee == "":
        normalized_assignee = None
    now = now or utc_now_datetime()
    created_at = utc_text(now)
    with db.transaction() as conn:
        if idempotency_key:
            replay = conn.execute(
                "SELECT action_id FROM moderation_actions WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if replay is not None:
                case = conn.execute(
                    "SELECT * FROM moderation_cases WHERE case_id = ?",
                    (case_id,),
                ).fetchone()
                if case is None:
                    raise KeyError(case_id)
                return dict(case)
        case = conn.execute(
            "SELECT * FROM moderation_cases WHERE case_id = ?",
            (case_id,),
        ).fetchone()
        if case is None:
            raise KeyError(case_id)
        previous_assignee = (
            str(case["assigned_to"]) if case["assigned_to"] is not None else None
        )
        conn.execute(
            """
            UPDATE moderation_cases
            SET assigned_to = ?, updated_at = ?
            WHERE case_id = ?
            """,
            (normalized_assignee, created_at, case_id),
        )
        conn.execute(
            """
            INSERT INTO moderation_actions(
                action_id, case_id, target_type, target_id, actor_type, actor_id,
                action, previous_state, new_state, reason, duration_seconds,
                idempotency_key, reverses_action_id, metadata_json, created_at
            )
            VALUES (?, ?, ?, ?, 'operator', ?, 'assign', ?, ?, ?, NULL, ?, NULL, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                case_id,
                case["target_type"],
                case["target_id"],
                actor_id,
                case["state"],
                case["state"],
                reason.strip()[:2000],
                idempotency_key,
                _json(
                    {
                        "previous_assigned_to": previous_assignee,
                        "assigned_to": normalized_assignee,
                    }
                ),
                created_at,
            ),
        )
        updated = conn.execute(
            "SELECT * FROM moderation_cases WHERE case_id = ?",
            (case_id,),
        ).fetchone()
    return dict(updated)


def is_content_control_active(
    db: Any,
    *,
    target_type: str,
    target_id: str,
    control_type: str = "quarantine",
) -> bool:
    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM moderation_content_controls
            WHERE target_type = ?
              AND target_id = ?
              AND control_type = ?
              AND active = 1
            LIMIT 1
            """,
            (str(target_type), str(target_id), str(control_type)),
        ).fetchone()
    return row is not None


def transition_case(
    db: Any,
    *,
    case_id: str,
    action: str,
    actor_id: str,
    reason: str,
    duration_seconds: int | None = None,
    idempotency_key: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    state_for_action = {
        "observe": "observed",
        "dismiss": "cleared",
        "timeout": "timed_out",
        "restrict": "restricted",
        "quarantine": "quarantined",
        "ban": "banned",
    }
    if action not in state_for_action:
        raise ValueError("Unsupported case transition.")
    if not reason.strip():
        raise ValueError("A moderator reason is required.")
    if action == "timeout":
        if (
            duration_seconds is None
            or not MIN_TIMEOUT_SECONDS <= int(duration_seconds) <= MAX_TIMEOUT_SECONDS
        ):
            raise ValueError("Timeout duration must be between 1 second and 10 years.")
        duration_seconds = int(duration_seconds)
    elif duration_seconds is not None:
        raise ValueError("This action does not accept a duration.")
    now = now or utc_now_datetime()
    created_at = utc_text(now)
    with db.transaction() as conn:
        if idempotency_key:
            existing_action = conn.execute(
                "SELECT action_id FROM moderation_actions WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing_action is not None:
                existing_case = conn.execute(
                    "SELECT * FROM moderation_cases WHERE case_id = ?",
                    (case_id,),
                ).fetchone()
                if existing_case is None:
                    raise KeyError(case_id)
                return dict(existing_case)
        case = conn.execute(
            "SELECT * FROM moderation_cases WHERE case_id = ?",
            (case_id,),
        ).fetchone()
        if case is None:
            raise KeyError(case_id)
        target_type = str(case["target_type"])
        target_id = str(case["target_id"])
        if action in {"timeout", "restrict", "ban"}:
            if target_type != "player":
                raise ValueError(f"Only a player case can apply an account {action}.")
            target = conn.execute(
                "SELECT is_coach FROM players WHERE player_id = ?",
                (target_id,),
            ).fetchone()
            if target is None:
                raise ValueError("The target player no longer exists.")
            if bool(target["is_coach"]):
                raise ValueError("Coach cannot receive moderation sanctions.")
        if action == "quarantine" and target_type not in {
            "room",
            "invention",
            "player_event",
            "image",
        }:
            raise ValueError(
                "Quarantine is supported only for room, invention, player-event, and image cases."
            )

        # Expired timeouts stop blocking later case actions even if no player
        # request happened to trigger the ordinary sanction cleanup.
        conn.execute(
            """
            UPDATE moderation_sanctions
            SET active = 0, updated_at = ?
            WHERE case_id = ?
              AND active = 1
              AND expires_at IS NOT NULL
              AND expires_at <= ?
            """,
            (created_at, case_id, created_at),
        )
        active_sanction = conn.execute(
            """
            SELECT sanction_id
            FROM moderation_sanctions
            WHERE case_id = ? AND active = 1
            LIMIT 1
            """,
            (case_id,),
        ).fetchone()
        active_content_control = conn.execute(
            """
            SELECT control_id
            FROM moderation_content_controls
            WHERE case_id = ? AND active = 1
            LIMIT 1
            """,
            (case_id,),
        ).fetchone()
        if action in {"observe", "dismiss"} and (
            active_sanction is not None or active_content_control is not None
        ):
            raise ValueError(
                "Reverse or restore the active moderation control before observing or dismissing this case."
            )
        if action in {"timeout", "restrict", "ban"} and active_sanction is not None:
            raise ValueError("This case already has an active account sanction.")
        if action == "quarantine" and active_content_control is not None:
            raise ValueError("This case already has an active content quarantine.")

        previous_state = str(case["state"])
        new_state = state_for_action[action]
        closed_at = created_at if action == "dismiss" else None
        conn.execute(
            """
            UPDATE moderation_cases
            SET state = ?, updated_at = ?, closed_at = ?
            WHERE case_id = ?
            """,
            (new_state, created_at, closed_at, case_id),
        )
        action_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO moderation_actions(
                action_id, case_id, target_type, target_id, actor_type, actor_id,
                action, previous_state, new_state, reason, duration_seconds,
                idempotency_key, reverses_action_id, metadata_json, created_at
            )
            VALUES (?, ?, ?, ?, 'operator', ?, ?, ?, ?, ?, ?, ?, NULL, '{}', ?)
            """,
            (
                action_id,
                case_id,
                case["target_type"],
                case["target_id"],
                actor_id,
                action,
                previous_state,
                new_state,
                reason.strip()[:2000],
                duration_seconds,
                idempotency_key,
                created_at,
            ),
        )
        if action in {"timeout", "restrict", "ban"}:
            expires_at = (
                utc_text(now + timedelta(seconds=int(duration_seconds)))
                if action == "timeout"
                else None
            )
            conn.execute(
                """
                INSERT INTO moderation_sanctions(
                    sanction_id, case_id, target_player_id, sanction_type, scope,
                    active, starts_at, expires_at, reason, created_by,
                    reversed_by_action_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 'account', 1, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    case_id,
                    target_id,
                    action,
                    created_at,
                    expires_at,
                    reason.strip()[:2000],
                    actor_id,
                    created_at,
                    created_at,
                ),
            )
        elif action == "quarantine":
            conn.execute(
                """
                INSERT INTO moderation_content_controls(
                    control_id, case_id, originating_action_id, target_type,
                    target_id, control_type, active, reason, created_by,
                    reversed_by_action_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'quarantine', 1, ?, ?, NULL, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    case_id,
                    action_id,
                    target_type,
                    target_id,
                    reason.strip()[:2000],
                    actor_id,
                    created_at,
                    created_at,
                ),
            )
        updated = conn.execute(
            "SELECT * FROM moderation_cases WHERE case_id = ?",
            (case_id,),
        ).fetchone()
    return dict(updated)


def reverse_case_action(
    db: Any,
    *,
    case_id: str,
    actor_id: str,
    reason: str,
    action_id: str | None = None,
    idempotency_key: str | None = None,
    reversal_action: str = "reverse",
    now: datetime | None = None,
) -> dict[str, Any]:
    if not reason.strip():
        raise ValueError("A moderator reason is required.")
    if reversal_action not in {"reverse", "restore"}:
        raise ValueError("Unsupported reversal action.")
    now = now or utc_now_datetime()
    created_at = utc_text(now)
    with db.transaction() as conn:
        if idempotency_key:
            replay = conn.execute(
                "SELECT action_id FROM moderation_actions WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if replay is not None:
                case = conn.execute(
                    "SELECT * FROM moderation_cases WHERE case_id = ?",
                    (case_id,),
                ).fetchone()
                if case is None:
                    raise KeyError(case_id)
                return dict(case)
        case = conn.execute(
            "SELECT * FROM moderation_cases WHERE case_id = ?",
            (case_id,),
        ).fetchone()
        if case is None:
            raise KeyError(case_id)
        if action_id:
            original = conn.execute(
                """
                SELECT * FROM moderation_actions
                WHERE action_id = ? AND case_id = ?
                """,
                (action_id, case_id),
            ).fetchone()
        else:
            original = conn.execute(
                """
                SELECT ma.*
                FROM moderation_actions AS ma
                WHERE ma.case_id = ?
                  AND ma.action IN ('timeout', 'restrict', 'ban', 'quarantine')
                  AND NOT EXISTS (
                      SELECT 1 FROM moderation_actions AS reversal
                      WHERE reversal.reverses_action_id = ma.action_id
                  )
                ORDER BY ma.created_at DESC
                LIMIT 1
                """,
                (case_id,),
            ).fetchone()
        if original is None:
            raise ValueError("No reversible moderation action was found.")
        original_action = str(original["action"])
        if original_action not in {"timeout", "restrict", "ban", "quarantine"}:
            raise ValueError("The selected moderation action is not reversible.")
        already_reversed = conn.execute(
            "SELECT action_id FROM moderation_actions WHERE reverses_action_id = ?",
            (original["action_id"],),
        ).fetchone()
        if already_reversed is not None:
            raise ValueError("The selected moderation action was already reversed.")
        reversal_id = str(uuid.uuid4())
        if original_action in {"timeout", "restrict", "ban"}:
            conn.execute(
                """
                UPDATE moderation_sanctions
                SET active = 0, reversed_by_action_id = ?, updated_at = ?
                WHERE case_id = ?
                  AND sanction_type = ?
                  AND active = 1
                """,
                (reversal_id, created_at, case_id, original_action),
            )
        if original_action == "ban":
            conn.execute(
                """
                UPDATE players
                SET is_banned = 0, banned_at = NULL, ban_reason = NULL,
                    updated_at = ?
                WHERE player_id = ?
                """,
                (created_at, case["target_id"]),
            )
            conn.execute(
                """
                UPDATE bans
                SET active = 0, updated_at = ?
                WHERE player_id = ?
                """,
                (created_at, case["target_id"]),
            )
        elif original_action == "quarantine":
            conn.execute(
                """
                UPDATE moderation_content_controls
                SET active = 0, updated_at = ?
                WHERE case_id = ?
                  AND originating_action_id = ?
                  AND control_type = 'quarantine'
                  AND active = 1
                """,
                (
                    created_at,
                    case_id,
                    original["action_id"],
                ),
            )
        restored_state = str(original["previous_state"] or "observed")
        conn.execute(
            """
            UPDATE moderation_cases
            SET state = ?, updated_at = ?, closed_at = NULL
            WHERE case_id = ?
            """,
            (restored_state, created_at, case_id),
        )
        conn.execute(
            """
            INSERT INTO moderation_actions(
                action_id, case_id, target_type, target_id, actor_type, actor_id,
                action, previous_state, new_state, reason, duration_seconds,
                idempotency_key, reverses_action_id, metadata_json, created_at
            )
            VALUES (?, ?, ?, ?, 'operator', ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
            """,
            (
                reversal_id,
                case_id,
                case["target_type"],
                case["target_id"],
                actor_id,
                reversal_action,
                case["state"],
                restored_state,
                reason.strip()[:2000],
                idempotency_key,
                original["action_id"],
                _json({"reversed_action": original_action}),
                created_at,
            ),
        )
        if original_action == "quarantine":
            conn.execute(
                """
                UPDATE moderation_content_controls
                SET reversed_by_action_id = ?, updated_at = ?
                WHERE case_id = ?
                  AND originating_action_id = ?
                  AND control_type = 'quarantine'
                """,
                (
                    reversal_id,
                    created_at,
                    case_id,
                    original["action_id"],
                ),
            )
        updated = conn.execute(
            "SELECT * FROM moderation_cases WHERE case_id = ?",
            (case_id,),
        ).fetchone()
    return dict(updated)


def active_player_sanction(
    db: Any,
    player_id: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    now_text = utc_text(now)
    with db.connection() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM moderation_sanctions
            WHERE target_player_id = ?
              AND active = 1
              AND scope = 'account'
              AND starts_at <= ?
              AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY
                CASE sanction_type WHEN 'ban' THEN 0 WHEN 'timeout' THEN 1 ELSE 2 END,
                created_at DESC
            LIMIT 1
            """,
            (player_id, now_text, now_text),
        ).fetchone()
    return _row_dict(row)


def active_player_sanctions(
    db: Any,
    player_id: str,
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    now_text = utc_text(now)
    with db.connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM moderation_sanctions
            WHERE target_player_id = ?
              AND active = 1
              AND starts_at <= ?
              AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY created_at DESC
            """,
            (player_id, now_text, now_text),
        ).fetchall()
    return [dict(row) for row in rows]


def has_active_player_scope(
    db: Any,
    player_id: str,
    scope: str,
    *,
    now: datetime | None = None,
) -> bool:
    return any(
        str(row.get("scope")) == str(scope)
        for row in active_player_sanctions(db, player_id, now=now)
    )


def set_player_scope_restriction(
    db: Any,
    *,
    player_id: str,
    scope: str,
    restrict: bool,
    actor_id: str,
    reason: str,
    case_id: str | None = None,
    idempotency_key: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    supported_scopes = {
        "invention_publishing": (
            "restrict_invention_publishing",
            "restore_invention_publishing",
        ),
        "room_publishing": (
            "restrict_room_publishing",
            "restore_room_publishing",
        ),
        "chat": ("restrict_chat", "restore_chat"),
    }
    if scope not in supported_scopes:
        raise ValueError("Unsupported player restriction scope.")
    if not reason.strip():
        raise ValueError("A moderator reason is required.")
    now = now or utc_now_datetime()
    created_at = utc_text(now)
    action = supported_scopes[scope][0 if restrict else 1]
    with db.transaction() as conn:
        if idempotency_key:
            replay = conn.execute(
                "SELECT * FROM moderation_actions WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if replay is not None:
                return dict(replay)
        player = conn.execute(
            "SELECT player_id, is_coach FROM players WHERE player_id = ?",
            (player_id,),
        ).fetchone()
        if player is None:
            raise KeyError(player_id)
        if bool(player["is_coach"]):
            raise ValueError("Coach cannot receive moderation restrictions.")
        current = conn.execute(
            """
            SELECT *
            FROM moderation_sanctions
            WHERE target_player_id = ? AND scope = ? AND active = 1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (player_id, scope),
        ).fetchone()
        if restrict and current is not None:
            raise ValueError("This player already has that active restriction.")
        if not restrict and current is None:
            raise ValueError("This player does not have that active restriction.")
        original_action_id: str | None = None
        if current is not None:
            original_action = conn.execute(
                """
                SELECT action_id
                FROM moderation_actions
                WHERE target_type = 'player'
                  AND target_id = ?
                  AND action = ?
                  AND created_at <= ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (player_id, supported_scopes[scope][0], current["created_at"]),
            ).fetchone()
            if original_action is not None:
                original_action_id = str(original_action["action_id"])
        action_id = str(uuid.uuid4())
        previous_state = "restricted" if current is not None else "normal"
        new_state = "restricted" if restrict else "normal"
        conn.execute(
            """
            INSERT INTO moderation_actions(
                action_id, case_id, target_type, target_id, actor_type, actor_id,
                action, previous_state, new_state, reason, duration_seconds,
                idempotency_key, reverses_action_id, metadata_json, created_at
            )
            VALUES (?, ?, 'player', ?, 'operator', ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
            """,
            (
                action_id,
                case_id,
                player_id,
                actor_id,
                action,
                previous_state,
                new_state,
                reason.strip()[:2000],
                idempotency_key,
                original_action_id,
                _json({"scope": scope}),
                created_at,
            ),
        )
        if restrict:
            conn.execute(
                """
                INSERT INTO moderation_sanctions(
                    sanction_id, case_id, target_player_id, sanction_type, scope,
                    active, starts_at, expires_at, reason, created_by,
                    reversed_by_action_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 1, ?, NULL, ?, ?, NULL, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    case_id,
                    player_id,
                    action,
                    scope,
                    created_at,
                    reason.strip()[:2000],
                    actor_id,
                    created_at,
                    created_at,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE moderation_sanctions
                SET active = 0, reversed_by_action_id = ?, updated_at = ?
                WHERE sanction_id = ?
                """,
                (action_id, created_at, current["sanction_id"]),
            )
        row = conn.execute(
            "SELECT * FROM moderation_actions WHERE action_id = ?",
            (action_id,),
        ).fetchone()
    return dict(row)
