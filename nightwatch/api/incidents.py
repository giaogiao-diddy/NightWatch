from datetime import datetime, timezone
import logging
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, status

from nightwatch.agent.graph import build_graph, build_runtime_config
from nightwatch.agent.runtime_store import (
    initialize_runtime_store,
    sync_followup_for_state,
    write_incident_audit_log,
)
from nightwatch.api.deps import require_api_key
from nightwatch.metrics import inc_incidents_total
from nightwatch.models.api import IncidentIngestRequest, IncidentResponse
from nightwatch.models.incident import IncidentEvent, LogSnippet
from nightwatch.models.state import AgentState


router = APIRouter(
    prefix="/api/v1/incidents",
    tags=["incidents"],
    dependencies=[Depends(require_api_key)],
)
logger = logging.getLogger(__name__)

try:
    initialize_runtime_store()
except Exception:  # noqa: BLE001
    logger.exception("failed to initialize runtime store for incidents api")


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
        logger.exception("failed to write incident audit log")


def _safe_sync_followup(state: AgentState, *, reason: str) -> None:
    try:
        sync_followup_for_state(state, reason=reason)
    except Exception:  # noqa: BLE001
        logger.exception("failed to sync incident followup")


def _build_incident_event(request: IncidentIngestRequest) -> IncidentEvent:
    payload = request.payload
    event_time = payload.event_time or datetime.now(timezone.utc)

    return IncidentEvent(
        incident_id=f"inc_{uuid4().hex}",
        source=request.source,
        incident_type=payload.incident_type,
        severity=payload.severity,
        cluster=payload.cluster,
        job_name=payload.job_name,
        application_id=payload.application_id,
        scheduler_task_id=payload.scheduler_task_id,
        event_time=event_time,
        error_signature=payload.message,
        labels=payload.labels,
        log_snippets=[
            LogSnippet(
                source=request.source,
                host=payload.host,
                offset_start=0,
                offset_end=1,
                content=payload.message,
                score=1.0,
            )
        ],
    )


def _write_key_transition_audits(state: AgentState) -> None:
    diagnosis = state.diagnosis or {}
    guardrail_result = state.guardrail_result or {}

    if diagnosis:
        _safe_write_audit(
            state,
            stage="diagnose_completed",
            status="completed",
            payload={
                "incident_type": str(diagnosis.get("incident_type", state.incident.incident_type.value)),
                "risk_level": str(diagnosis.get("risk_level", "")),
                "requires_circuit_break": bool(diagnosis.get("requires_circuit_break", False)),
            },
        )

    if guardrail_result:
        _safe_write_audit(
            state,
            stage="guardrail_triggered",
            status=str(guardrail_result.get("decision", "unknown")),
            payload={
                "approval_status": str(guardrail_result.get("approval_status", "")),
                "reason": str(guardrail_result.get("reason", "")),
            },
        )

    execution_records = state.execution_records or []
    for record in execution_records:
        action_type = str(record.get("action_type", "")).strip()
        if not action_type:
            continue
        if action_type == "notify_owners":
            continue

        _safe_write_audit(
            state,
            stage="execution_completed",
            status=str(record.get("result", "unknown")),
            payload={
                "action_type": action_type,
                "target_system": str(record.get("target_system", "")),
                "target_id": str(record.get("target_id", "")),
            },
        )


@router.post(
    "/ingest",
    response_model=IncidentResponse,
    status_code=status.HTTP_200_OK,
    summary="接入并处理告警事件",
    description="接收告警事件后初始化 AgentState，并异步执行 LangGraph 状态机主链路。",
    response_description="结构化的处理结果摘要",
)
async def ingest_incident(request: IncidentIngestRequest) -> IncidentResponse:
    """Ingest one incident and run the end-to-end handling graph asynchronously."""

    inc_incidents_total(request.source)

    incident = _build_incident_event(request)
    initial_state = AgentState(
        incident=incident,
        mode="auto" if request.auto_execute else "assist",
        conversation_id=incident.incident_id,
    )
    runtime_config = build_runtime_config(initial_state)

    _safe_write_audit(
        initial_state,
        stage="incident_ingested",
        status="accepted",
        payload={
            "source": request.source,
            "auto_execute": request.auto_execute,
            "mode": initial_state.mode,
        },
    )

    try:
        graph = build_graph()
        result = await graph.ainvoke(initial_state, config=runtime_config)
    except Exception as exc:  # noqa: BLE001
        failed_state = initial_state.model_copy(deep=True)
        failed_state.status = "failed"
        failed_state.error_message = str(exc)
        _safe_write_audit(
            failed_state,
            stage="graph_execution",
            status="failed",
            payload={"error": str(exc)},
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"failed to execute incident graph: {exc}",
        ) from exc

    final_state = AgentState.model_validate(result)
    _write_key_transition_audits(final_state)
    _safe_sync_followup(final_state, reason="api_post_run")

    _safe_write_audit(
        final_state,
        stage="graph_execution",
        status=final_state.status,
        payload={
            "retry_count": final_state.retry_count,
            "execution_record_count": len(final_state.execution_records),
        },
    )

    diagnosis = final_state.diagnosis or {}
    heal_plan = final_state.heal_plan or {}
    guardrail_result = final_state.guardrail_result or {}

    return IncidentResponse(
        incident_id=incident.incident_id,
        status=final_state.status,
        incident_type=str(diagnosis.get("incident_type", incident.incident_type.value)),
        severity=incident.severity,
        diagnosis_summary=str(diagnosis.get("summary", "")),
        recommended_action=str(heal_plan.get("action_type", "")),
        approval_required=guardrail_result.get("decision") == "require_approval",
        execution_result=final_state.execution_records[-1] if final_state.execution_records else None,
        state_snapshot={
            "mode": final_state.mode,
            "status": final_state.status,
            "diagnosis": diagnosis,
            "heal_plan": heal_plan,
            "blast_radius": final_state.blast_radius or {},
            "guardrail_result": guardrail_result,
            "notification_targets": final_state.notification_targets,
            "notification_payloads": final_state.notification_payloads,
            "execution_records": final_state.execution_records,
            "diagnosis_errors": final_state.diagnosis_errors,
        },
        error=final_state.error_message or None,
    )
