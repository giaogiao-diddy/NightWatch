import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel

from nightwatch.agent.graph import resolve_thread_id
from nightwatch.config import get_settings
from nightwatch.models.state import AgentState


logger = logging.getLogger(__name__)


class FollowupWorkItem(BaseModel):
    queue_id: int
    thread_id: str
    incident_id: str
    conversation_id: str
    queue_status: str
    attempts: int
    max_attempts: int
    state: AgentState


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _to_iso_with_delay(seconds: int) -> str:
    return (_utc_now() + timedelta(seconds=max(seconds, 0))).isoformat()


def _connect() -> sqlite3.Connection:
    settings = get_settings()
    connection = sqlite3.connect(
        settings.get_worker_runtime_sqlite_path(),
        timeout=30,
        isolation_level=None,
        check_same_thread=False,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def initialize_runtime_store() -> None:
    with _connect() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS incident_followups (
                queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL UNIQUE,
                incident_id TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                queue_status TEXT NOT NULL,
                state_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                next_run_at TEXT NOT NULL,
                lease_until TEXT,
                owner TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_incident_followups_status_due
            ON incident_followups(queue_status, next_run_at)
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS incident_audit_logs (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                incident_id TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_incident_audit_logs_incident
            ON incident_audit_logs(incident_id, created_at)
            """
        )


def _classify_followup(state: AgentState) -> tuple[str | None, int, str]:
    settings = get_settings()
    guardrail_result = state.guardrail_result or {}
    guardrail_decision = str(guardrail_result.get("decision", "")).strip().lower()
    approval_status = str(guardrail_result.get("approval_status", "")).strip().lower()

    if guardrail_decision == "require_approval" and approval_status != "approved":
        return (
            "waiting_approval",
            settings.worker_approval_recheck_seconds,
            "awaiting_human_approval",
        )

    if state.status in {"pending", "diagnosed"}:
        return (
            "pending",
            settings.worker_retry_backoff_seconds,
            "state_not_terminal",
        )

    return None, 0, ""


def write_incident_audit_log(
    state: AgentState,
    *,
    stage: str,
    status: str,
    payload: dict[str, Any] | None = None,
) -> None:
    normalized_stage = stage.strip() or "unknown_stage"
    normalized_status = status.strip() or "unknown_status"
    thread_id = resolve_thread_id(state)

    if not thread_id:
        return

    normalized_payload = dict(payload or {})
    log_entry = {
        "ts": _utc_now_iso(),
        "incident_id": state.incident.incident_id,
        "conversation_id": state.conversation_id,
        "thread_id": thread_id,
        "stage": normalized_stage,
        "status": normalized_status,
        "payload": normalized_payload,
    }
    logger.info("incident_audit %s", json.dumps(log_entry, sort_keys=True, ensure_ascii=True))

    with _connect() as connection:
        connection.execute(
            """
            INSERT INTO incident_audit_logs(
                incident_id,
                conversation_id,
                thread_id,
                stage,
                status,
                payload_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                state.incident.incident_id,
                state.conversation_id,
                thread_id,
                normalized_stage,
                normalized_status,
                json.dumps(normalized_payload, sort_keys=True, ensure_ascii=True),
                _utc_now_iso(),
            ),
        )


def sync_followup_for_state(state: AgentState, *, reason: str) -> None:
    thread_id = resolve_thread_id(state)
    if not thread_id:
        return

    followup_status, delay_seconds, followup_reason = _classify_followup(state)
    state_json = state.model_dump_json()
    now_iso = _utc_now_iso()

    with _connect() as connection:
        if followup_status is None:
            connection.execute(
                """
                UPDATE incident_followups
                SET queue_status = 'completed',
                    state_json = ?,
                    lease_until = NULL,
                    owner = '',
                    updated_at = ?,
                    next_run_at = ?,
                    last_error = ''
                WHERE thread_id = ?
                """,
                (state_json, now_iso, now_iso, thread_id),
            )
            return

        settings = get_settings()
        normalized_reason = reason.strip() or followup_reason
        connection.execute(
            """
            INSERT INTO incident_followups(
                thread_id,
                incident_id,
                conversation_id,
                queue_status,
                state_json,
                attempts,
                max_attempts,
                next_run_at,
                lease_until,
                owner,
                last_error,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, 0, ?, ?, NULL, '', ?, ?, ?)
            ON CONFLICT(thread_id) DO UPDATE SET
                incident_id = excluded.incident_id,
                conversation_id = excluded.conversation_id,
                queue_status = excluded.queue_status,
                state_json = excluded.state_json,
                max_attempts = excluded.max_attempts,
                next_run_at = excluded.next_run_at,
                lease_until = NULL,
                owner = '',
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            """,
            (
                thread_id,
                state.incident.incident_id,
                state.conversation_id,
                followup_status,
                state_json,
                settings.worker_max_attempts,
                _to_iso_with_delay(delay_seconds),
                normalized_reason,
                now_iso,
                now_iso,
            ),
        )


def claim_due_followups(worker_id: str, *, limit: int) -> list[FollowupWorkItem]:
    normalized_worker_id = worker_id.strip()
    if not normalized_worker_id:
        return []

    settings = get_settings()
    now_iso = _utc_now_iso()
    lease_until_iso = _to_iso_with_delay(settings.worker_claim_ttl_seconds)

    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            UPDATE incident_followups
            SET queue_status = 'pending',
                lease_until = NULL,
                owner = '',
                updated_at = ?
            WHERE queue_status IN ('running', 'api_processing')
              AND lease_until IS NOT NULL
              AND lease_until <= ?
            """,
            (now_iso, now_iso),
        )

        rows = connection.execute(
            """
            SELECT queue_id,
                   thread_id,
                   incident_id,
                   conversation_id,
                   queue_status,
                   state_json,
                   attempts,
                   max_attempts
            FROM incident_followups
            WHERE queue_status IN ('pending', 'waiting_approval')
              AND next_run_at <= ?
            ORDER BY next_run_at ASC, queue_id ASC
            LIMIT ?
            """,
            (now_iso, limit),
        ).fetchall()

        if not rows:
            connection.execute("COMMIT")
            return []

        queue_ids = [int(row["queue_id"]) for row in rows]
        placeholders = ",".join("?" for _ in queue_ids)
        connection.execute(
            f"""
            UPDATE incident_followups
            SET queue_status = 'running',
                owner = ?,
                lease_until = ?,
                updated_at = ?
            WHERE queue_id IN ({placeholders})
            """,
            (normalized_worker_id, lease_until_iso, now_iso, *queue_ids),
        )
        connection.execute("COMMIT")

    work_items: list[FollowupWorkItem] = []
    for row in rows:
        try:
            item_state = AgentState.model_validate_json(str(row["state_json"]))
        except Exception:  # noqa: BLE001
            logger.exception("failed to deserialize followup state")
            continue

        work_items.append(
            FollowupWorkItem(
                queue_id=int(row["queue_id"]),
                thread_id=str(row["thread_id"]),
                incident_id=str(row["incident_id"]),
                conversation_id=str(row["conversation_id"]),
                queue_status=str(row["queue_status"]),
                attempts=int(row["attempts"]),
                max_attempts=int(row["max_attempts"]),
                state=item_state,
            )
        )

    return work_items


def release_worker_claims(worker_id: str) -> None:
    normalized_worker_id = worker_id.strip()
    if not normalized_worker_id:
        return

    now_iso = _utc_now_iso()
    with _connect() as connection:
        connection.execute(
            """
            UPDATE incident_followups
            SET queue_status = 'pending',
                lease_until = NULL,
                owner = '',
                updated_at = ?,
                next_run_at = ?
            WHERE queue_status = 'running'
              AND owner = ?
            """,
            (now_iso, now_iso, normalized_worker_id),
        )


def finalize_followup_attempt(
    item: FollowupWorkItem,
    *,
    state: AgentState,
    processing_error: str = "",
) -> str:
    settings = get_settings()
    attempts_after_run = item.attempts + 1
    normalized_error = processing_error.strip()

    if normalized_error:
        if attempts_after_run >= item.max_attempts:
            next_status = "failed"
            delay_seconds = 0
            last_error = f"max attempts exceeded: {normalized_error}"
        else:
            next_status = "pending"
            delay_seconds = settings.worker_retry_backoff_seconds
            last_error = normalized_error
    else:
        next_status, delay_seconds, followup_reason = _classify_followup(state)
        if next_status is None:
            next_status = "completed"
            delay_seconds = 0
            last_error = ""
        elif attempts_after_run >= item.max_attempts:
            next_status = "failed"
            delay_seconds = 0
            last_error = f"max attempts exceeded while {followup_reason}"
        else:
            last_error = followup_reason

    now_iso = _utc_now_iso()
    with _connect() as connection:
        connection.execute(
            """
            UPDATE incident_followups
            SET queue_status = ?,
                state_json = ?,
                attempts = ?,
                next_run_at = ?,
                lease_until = NULL,
                owner = '',
                last_error = ?,
                updated_at = ?
            WHERE queue_id = ?
            """,
            (
                next_status,
                state.model_dump_json(),
                attempts_after_run,
                _to_iso_with_delay(delay_seconds),
                last_error,
                now_iso,
                item.queue_id,
            ),
        )

    return next_status
