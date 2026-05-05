import argparse
import asyncio
import logging
import signal
import uuid

from nightwatch.agent.graph import (
    build_graph,
    build_runtime_config,
    list_checkpoint_states,
    recover_state_by_conversation_id,
    recover_state_by_incident_id,
    shutdown_graph_runtime,
)
from nightwatch.agent.runtime_store import (
    FollowupWorkItem,
    claim_due_followups,
    finalize_followup_attempt,
    initialize_runtime_store,
    release_worker_claims,
    sync_followup_for_state,
    write_incident_audit_log,
)
from nightwatch.config import get_settings
from nightwatch.models.state import AgentState


logger = logging.getLogger(__name__)


def load_state_for_incident(incident_id: str) -> AgentState | None:
    try:
        return recover_state_by_incident_id(incident_id)
    except Exception:  # noqa: BLE001
        logger.exception("failed to recover worker state by incident id")
        return None


def load_state_for_conversation(conversation_id: str) -> AgentState | None:
    try:
        return recover_state_by_conversation_id(conversation_id)
    except Exception:  # noqa: BLE001
        logger.exception("failed to recover worker state by conversation id")
        return None


class WorkerDaemon:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.worker_id = f"nightwatch-worker-{uuid.uuid4().hex[:8]}"
        self._stop_event = asyncio.Event()
        self._graph = None

    def request_stop(self, reason: str) -> None:
        if self._stop_event.is_set():
            return

        logger.info("worker stop requested: %s", reason)
        self._stop_event.set()

    @staticmethod
    def _safe_write_audit(
        state: AgentState,
        *,
        stage: str,
        status: str,
        payload: dict[str, object] | None = None,
    ) -> None:
        try:
            write_incident_audit_log(
                state,
                stage=stage,
                status=status,
                payload=payload,
            )
        except Exception:  # noqa: BLE001
            logger.exception("failed to write worker audit log")

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        def _handle_signal(signum: int) -> None:
            self.request_stop(reason=f"signal_{signum}")

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda current=sig: _handle_signal(current.value))
            except NotImplementedError:
                signal.signal(sig, lambda signum, _frame: _handle_signal(signum))

    async def _seed_followups_from_checkpoints(self) -> int:
        seeded_count = 0
        for thread_id, state in list_checkpoint_states(limit=500):
            try:
                sync_followup_for_state(state, reason="checkpoint_recovery")
                seeded_count += 1
            except Exception:  # noqa: BLE001
                logger.exception("failed to sync followup from checkpoint thread_id=%s", thread_id)
        return seeded_count

    async def _run_graph_for_item(self, item: FollowupWorkItem) -> AgentState:
        if self._graph is None:
            self._graph = build_graph()

        state = item.state.model_copy(deep=True)
        runtime_config = build_runtime_config(state)
        runtime_config.setdefault("configurable", {})["thread_id"] = item.thread_id

        result = await self._graph.ainvoke(state, config=runtime_config)
        return AgentState.model_validate(result)

    async def _process_item(self, item: FollowupWorkItem) -> None:
        working_state = item.state.model_copy(deep=True)
        self._safe_write_audit(
            working_state,
            stage="worker_followup_started",
            status="running",
            payload={
                "queue_id": item.queue_id,
                "attempt": item.attempts + 1,
            },
        )

        try:
            final_state = await self._run_graph_for_item(item)
            self._safe_write_audit(
                final_state,
                stage="worker_followup_graph_completed",
                status=final_state.status,
                payload={
                    "queue_id": item.queue_id,
                    "attempt": item.attempts + 1,
                },
            )

            next_status = finalize_followup_attempt(item, state=final_state)
            self._safe_write_audit(
                final_state,
                stage="worker_followup_finalized",
                status=next_status,
                payload={
                    "queue_id": item.queue_id,
                    "attempt": item.attempts + 1,
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("worker failed to process followup queue_id=%s", item.queue_id)
            if str(exc):
                working_state.error_message = str(exc)
                working_state.diagnosis_errors.append(str(exc))

            next_status = finalize_followup_attempt(
                item,
                state=working_state,
                processing_error=str(exc),
            )
            self._safe_write_audit(
                working_state,
                stage="worker_followup_finalized",
                status=next_status,
                payload={
                    "queue_id": item.queue_id,
                    "attempt": item.attempts + 1,
                    "error": str(exc),
                },
            )

    async def run(self, *, once: bool = False) -> None:
        if not self.settings.worker_enable:
            logger.info("worker disabled by configuration")
            return

        initialize_runtime_store()
        self._install_signal_handlers()
        if self._graph is None:
            self._graph = build_graph()

        seeded_count = await self._seed_followups_from_checkpoints()
        logger.info("worker initialized worker_id=%s seeded_followups=%s", self.worker_id, seeded_count)

        try:
            while not self._stop_event.is_set():
                items = claim_due_followups(
                    self.worker_id,
                    limit=self.settings.worker_batch_size,
                )

                if not items:
                    if once:
                        return
                    await asyncio.sleep(self.settings.worker_poll_interval_seconds)
                    continue

                for item in items:
                    if self._stop_event.is_set():
                        break
                    await self._process_item(item)

                if once:
                    return
        finally:
            release_worker_claims(self.worker_id)
            shutdown_graph_runtime()
            logger.info("worker shutdown completed worker_id=%s", self.worker_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="NightWatch background worker")
    parser.add_argument("--once", action="store_true", help="process one polling batch and exit")
    args = parser.parse_args()

    daemon = WorkerDaemon()
    asyncio.run(daemon.run(once=args.once))


if __name__ == "__main__":
    main()