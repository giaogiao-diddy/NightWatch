import asyncio
import atexit
import logging
import weakref
from collections.abc import Awaitable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from nightwatch.agent.nodes import (
    circuit_break,
    diagnose_rca,
    execute_heal,
    guardrail_check,
    human_approval,
    ingest_alert,
    notify_owners,
    plan_heal,
    retrieve_cases,
    verify_recovery,
)
from nightwatch.config import get_settings
from nightwatch.models.state import AgentState


try:
    from langgraph.checkpoint.sqlite import SqliteSaver
except ImportError:  # pragma: no cover - optional runtime dependency
    SqliteSaver = None  # type: ignore[assignment]

try:
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
except ImportError:  # pragma: no cover - optional runtime dependency
    AsyncSqliteSaver = None  # type: ignore[assignment]

try:
    import aiosqlite
except ImportError:  # pragma: no cover - optional runtime dependency
    aiosqlite = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)

_INCIDENT_THREAD_INDEX: dict[str, str] = {}
_CONVERSATION_THREAD_INDEX: dict[str, str] = {}
_SYNC_CHECKPOINTER: Any | None = None
_SYNC_CHECKPOINTER_CONTEXT: Any | None = None
_ASYNC_CHECKPOINTERS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Any] = weakref.WeakKeyDictionary()


def _run_coro_sync(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        return future.result()


def _close_checkpointer(checkpointer: Any | None, checkpointer_context: Any | None = None) -> None:
    if checkpointer_context is not None:
        exit_handler = getattr(checkpointer_context, "__exit__", None)
        if callable(exit_handler):
            try:
                exit_handler(None, None, None)
            except Exception:  # noqa: BLE001
                logger.exception("failed to close sqlite checkpointer context manager")

    if checkpointer is None:
        return

    connection = getattr(checkpointer, "conn", None)
    if connection is None:
        return

    close_handler = getattr(connection, "close", None)
    if callable(close_handler):
        try:
            close_result = close_handler()
            if asyncio.iscoroutine(close_result):
                _run_coro_sync(close_result)
        except Exception:  # noqa: BLE001
            logger.exception("failed to close sqlite checkpointer context")


def _close_sqlite_checkpointer_context() -> None:
    global _SYNC_CHECKPOINTER, _SYNC_CHECKPOINTER_CONTEXT

    sync_checkpointer = _SYNC_CHECKPOINTER
    sync_checkpointer_context = _SYNC_CHECKPOINTER_CONTEXT
    _SYNC_CHECKPOINTER = None
    _SYNC_CHECKPOINTER_CONTEXT = None

    _close_checkpointer(sync_checkpointer, sync_checkpointer_context)

    async_checkpointers = list(_ASYNC_CHECKPOINTERS.values())
    _ASYNC_CHECKPOINTERS.clear()
    for checkpointer in async_checkpointers:
        _close_checkpointer(checkpointer)


atexit.register(_close_sqlite_checkpointer_context)


def shutdown_graph_runtime() -> None:
    _close_sqlite_checkpointer_context()
    _INCIDENT_THREAD_INDEX.clear()
    _CONVERSATION_THREAD_INDEX.clear()


def resolve_thread_id(state: AgentState) -> str:
    incident_id = state.incident.incident_id.strip()
    conversation_id = state.conversation_id.strip()
    return conversation_id or incident_id


def _register_state_indices(state: AgentState, thread_id: str) -> None:
    incident_id = state.incident.incident_id.strip()
    conversation_id = state.conversation_id.strip()

    if incident_id:
        _INCIDENT_THREAD_INDEX[incident_id] = thread_id
    if conversation_id:
        _CONVERSATION_THREAD_INDEX[conversation_id] = thread_id


def _build_checkpointer():
    global _SYNC_CHECKPOINTER, _SYNC_CHECKPOINTER_CONTEXT

    settings = get_settings()
    backend = settings.graph_checkpointer_backend.strip().lower()

    if backend == "none":
        return None

    if backend == "sqlite":
        sqlite_path = settings.get_graph_checkpointer_sqlite_path()

        if AsyncSqliteSaver is not None and aiosqlite is not None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop is not None:
                checkpointer = _ASYNC_CHECKPOINTERS.get(loop)
                if checkpointer is None:
                    checkpointer = AsyncSqliteSaver(aiosqlite.connect(sqlite_path))
                    _ASYNC_CHECKPOINTERS[loop] = checkpointer
                return checkpointer

        if SqliteSaver is None:
            logger.warning("sqlite checkpointer requested but no sqlite saver is available; fallback to memory")
            return MemorySaver()

        if _SYNC_CHECKPOINTER is not None:
            return _SYNC_CHECKPOINTER

        _SYNC_CHECKPOINTER_CONTEXT = None

        if hasattr(SqliteSaver, "from_conn_string"):
            candidate = SqliteSaver.from_conn_string(sqlite_path)
            enter_handler = getattr(candidate, "__enter__", None)
            if callable(enter_handler):
                checkpointer = enter_handler()
                _SYNC_CHECKPOINTER_CONTEXT = candidate
                _SYNC_CHECKPOINTER = checkpointer
                return checkpointer
            _SYNC_CHECKPOINTER = candidate
            return candidate

        checkpointer = SqliteSaver(sqlite_path)
        _SYNC_CHECKPOINTER = checkpointer
        return checkpointer

    return MemorySaver()


def build_runtime_config(state: AgentState) -> dict[str, Any]:
    incident_id = state.incident.incident_id.strip()
    conversation_id = state.conversation_id.strip() or incident_id
    thread_id = resolve_thread_id(state)
    if not thread_id:
        thread_id = incident_id or conversation_id

    if not state.conversation_id and conversation_id:
        state.conversation_id = conversation_id

    _register_state_indices(state, thread_id=thread_id)

    return {
        "configurable": {
            "thread_id": thread_id,
        },
        "metadata": {
            "incident_id": incident_id,
            "conversation_id": conversation_id,
        },
    }


def _snapshot_to_state(snapshot: Any | None) -> AgentState | None:
    if snapshot is None:
        return None

    values = getattr(snapshot, "values", None)
    if values is None:
        return None

    try:
        return AgentState.model_validate(values)
    except Exception:  # noqa: BLE001
        return None


async def _aget_state_by_thread_id(thread_id: str) -> AgentState | None:
    normalized_thread_id = thread_id.strip()
    if not normalized_thread_id:
        return None

    graph = build_graph()
    config = {"configurable": {"thread_id": normalized_thread_id}}

    try:
        if hasattr(graph, "aget_state"):
            snapshot = await graph.aget_state(config)
        else:
            snapshot = graph.get_state(config)
    except Exception:  # noqa: BLE001
        return None

    state = _snapshot_to_state(snapshot)
    if state is not None:
        _register_state_indices(state, thread_id=normalized_thread_id)
    return state


def _get_state_by_thread_id(thread_id: str) -> AgentState | None:
    return _run_coro_sync(_aget_state_by_thread_id(thread_id))


def _scan_state_from_checkpoints(identifier: str, by: str) -> AgentState | None:
    for thread_id, state in list_checkpoint_states(limit=200):
        if by == "incident_id" and state.incident.incident_id == identifier:
            return state
        if by == "conversation_id" and state.conversation_id == identifier:
            return state

    return None


async def _alist_checkpoint_thread_ids(limit: int = 200) -> list[str]:
    checkpointer = _build_checkpointer()
    if checkpointer is None:
        return []

    checkpoint_items: list[Any]
    if hasattr(checkpointer, "alist"):
        try:
            checkpoint_items = [item async for item in checkpointer.alist(config=None, limit=limit)]
        except Exception:  # noqa: BLE001
            return []
    elif hasattr(checkpointer, "list"):
        try:
            checkpoint_items = list(checkpointer.list(config=None, limit=limit))
        except Exception:  # noqa: BLE001
            return []
    else:
        return []

    thread_ids: list[str] = []
    seen: set[str] = set()

    for item in checkpoint_items:
        config = getattr(item, "config", None)
        if not isinstance(config, dict):
            continue

        configurable = config.get("configurable", {})
        if not isinstance(configurable, dict):
            continue

        thread_id = str(configurable.get("thread_id", "")).strip()
        if not thread_id or thread_id in seen:
            continue

        seen.add(thread_id)
        thread_ids.append(thread_id)

    return thread_ids


def _iter_checkpoint_thread_ids(limit: int = 200) -> list[str]:
    return _run_coro_sync(_alist_checkpoint_thread_ids(limit=limit))


async def _alist_checkpoint_states(limit: int = 200) -> list[tuple[str, AgentState]]:
    results: list[tuple[str, AgentState]] = []
    thread_ids = await _alist_checkpoint_thread_ids(limit=limit)
    for thread_id in thread_ids:
        state = await _aget_state_by_thread_id(thread_id)
        if state is None:
            continue
        results.append((thread_id, state))
    return results


def list_checkpoint_states(limit: int = 200) -> list[tuple[str, AgentState]]:
    return _run_coro_sync(_alist_checkpoint_states(limit=limit))


def recover_state_by_incident_id(incident_id: str) -> AgentState | None:
    normalized_id = incident_id.strip()
    if not normalized_id:
        return None

    thread_id = _INCIDENT_THREAD_INDEX.get(normalized_id, normalized_id)
    state = _get_state_by_thread_id(thread_id)
    if state is not None and state.incident.incident_id == normalized_id:
        return state

    return _scan_state_from_checkpoints(identifier=normalized_id, by="incident_id")


def recover_state_by_conversation_id(conversation_id: str) -> AgentState | None:
    normalized_id = conversation_id.strip()
    if not normalized_id:
        return None

    thread_id = _CONVERSATION_THREAD_INDEX.get(normalized_id, normalized_id)
    state = _get_state_by_thread_id(thread_id)
    if state is not None and state.conversation_id == normalized_id:
        return state

    return _scan_state_from_checkpoints(identifier=normalized_id, by="conversation_id")


def route_after_diagnose_rca(state: AgentState | dict[str, Any]) -> str:
    normalized_state = AgentState.model_validate(state)
    if normalized_state.status == "failed":
        return "stop"

    diagnosis = normalized_state.diagnosis or {}
    if bool(diagnosis.get("requires_circuit_break", False)):
        return "circuit_break"

    return "continue_heal"


def route_after_guardrail_check(state: AgentState | dict[str, Any]) -> str:
    normalized_state = AgentState.model_validate(state)
    if normalized_state.status == "failed":
        return "stop"

    decision = (normalized_state.guardrail_result or {}).get("decision")
    if decision == "allow":
        return "execute"
    return "manual_gate"


def route_after_human_approval(state: AgentState | dict[str, Any]) -> str:
    normalized_state = AgentState.model_validate(state)
    approval_status = (normalized_state.guardrail_result or {}).get("approval_status")
    return "approved" if approval_status == "approved" else "rejected"


def route_after_execute_heal(state: AgentState | dict[str, Any]) -> str:
    normalized_state = AgentState.model_validate(state)
    if normalized_state.status == "diagnosed":
        return "verify"
    if normalized_state.status == "escalated":
        return "manual_intervention"
    return "stop"


def route_after_verify_recovery(state: AgentState | dict[str, Any]) -> str:
    normalized_state = AgentState.model_validate(state)

    if normalized_state.status == "healed":
        return "done"
    if normalized_state.status == "escalated":
        return "manual_intervention"
    return "stop"


def build_graph():
    workflow = StateGraph(AgentState)

    workflow.add_node("ingest_alert", ingest_alert)
    workflow.add_node("retrieve_cases", retrieve_cases)
    workflow.add_node("diagnose_rca", diagnose_rca)
    workflow.add_node("circuit_break", circuit_break)
    workflow.add_node("notify_owners", notify_owners)
    workflow.add_node("plan_heal", plan_heal)
    workflow.add_node("guardrail_check", guardrail_check)
    workflow.add_node("human_approval", human_approval)
    workflow.add_node("execute_heal", execute_heal)
    workflow.add_node("verify_recovery", verify_recovery)

    workflow.set_entry_point("ingest_alert")
    workflow.add_edge("ingest_alert", "retrieve_cases")
    workflow.add_edge("retrieve_cases", "diagnose_rca")
    workflow.add_conditional_edges(
        "diagnose_rca",
        route_after_diagnose_rca,
        {
            "circuit_break": "circuit_break",
            "continue_heal": "plan_heal",
            "stop": END,
        },
    )
    workflow.add_edge("circuit_break", "notify_owners")
    workflow.add_edge("notify_owners", END)
    workflow.add_edge("plan_heal", "guardrail_check")
    workflow.add_conditional_edges(
        "guardrail_check",
        route_after_guardrail_check,
        {
            "execute": "execute_heal",
            "manual_gate": "human_approval",
            "stop": END,
        },
    )
    workflow.add_conditional_edges(
        "human_approval",
        route_after_human_approval,
        {
            "approved": "execute_heal",
            "rejected": END,
        },
    )
    workflow.add_conditional_edges(
        "execute_heal",
        route_after_execute_heal,
        {
            "verify": "verify_recovery",
            "manual_intervention": "notify_owners",
            "stop": END,
        },
    )
    workflow.add_conditional_edges(
        "verify_recovery",
        route_after_verify_recovery,
        {
            "done": END,
            "manual_intervention": "notify_owners",
            "stop": END,
        },
    )

    return workflow.compile(checkpointer=_build_checkpointer())


async def run_minimal_loop(initial_state: AgentState) -> AgentState:
    graph = build_graph()
    config = build_runtime_config(initial_state)
    result = await graph.ainvoke(initial_state, config=config)
    final_state = AgentState.model_validate(result)

    thread_id = str(((config.get("configurable") or {}).get("thread_id", "")) or "")
    if thread_id:
        _register_state_indices(final_state, thread_id=thread_id)

    return final_state