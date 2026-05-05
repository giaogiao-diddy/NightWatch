import logging
import time
from typing import Any

from nightwatch.diagnostics.schema_drift import diagnose_schema_drift_incident
from nightwatch.diagnostics.spark_rca import (
    LLMServiceError as DiagnoseLLMServiceError,
    analyze_root_cause_with_llm,
    diagnose_spark_incident,
)
from nightwatch.healing.circuit_breaker import execute_circuit_break
from nightwatch.healing.guardrails import evaluate_guardrails, evaluate_human_approval
from nightwatch.healing.notifier import notify_accountability_owners
from nightwatch.healing.planner import (
    LLMServiceError as PlannerLLMServiceError,
    build_heal_plan,
    generate_heal_plan_with_llm,
)
from nightwatch.healing.verifier import verify_recovery as verify_heal_recovery
from nightwatch.healing.executor import execute_controlled_retry
from nightwatch.ingest.normalizer import normalize_incident
from nightwatch.lineage.blast_radius import analyze_blast_radius
from nightwatch.mcp.client import MCPToolCallError
from nightwatch.metrics import inc_action_success_total, observe_rca_duration_seconds
from nightwatch.models.api import RootCauseDiagnosis
from nightwatch.models.state import AgentState
from nightwatch.retrieval.retriever import hybrid_retrieve


logger = logging.getLogger(__name__)


def _build_incident_context(state: AgentState) -> dict[str, Any]:
    incident = state.incident
    retrieval_hints = [
        {
            "case_id": str(hit.get("case_id", "")),
            "root_cause": str(hit.get("root_cause", "")),
            "recommended_action": str(hit.get("recommended_action", "")),
            "score": float(hit.get("score", 0.0)),
        }
        for hit in state.retrieval_hits[:3]
    ]

    return {
        "incident_id": incident.incident_id,
        "source": incident.source,
        "cluster": incident.cluster,
        "job_name": incident.job_name,
        "application_id": incident.application_id,
        "event_time": incident.event_time.isoformat(),
        "error_signature": incident.error_signature,
        "retrieval_hints": retrieval_hints,
    }


def _build_retrieval_query(state: AgentState) -> str:
    incident = state.incident
    segments = [
        incident.source,
        incident.incident_type.value,
        incident.job_name,
        incident.error_signature,
        " ".join(snippet.content for snippet in incident.log_snippets[:2]),
    ]
    return " ".join(segment for segment in segments if segment).strip()


def _merge_schema_drift_diagnosis(
    diagnosis: dict[str, Any],
    schema_drift_diagnosis: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(diagnosis)
    merged.update(schema_drift_diagnosis)

    llm_evidence = [str(item) for item in diagnosis.get("evidence", [])]
    drift_evidence = [str(item) for item in schema_drift_diagnosis.get("evidence", [])]
    merged["evidence"] = list(dict.fromkeys(drift_evidence + llm_evidence))
    return merged


def _collect_schema_drift_table_name(state: AgentState) -> str:
    diagnosis = state.diagnosis or {}
    detail = diagnosis.get("schema_drift_detail", {})
    if isinstance(detail, dict):
        table_name = str(detail.get("table_name", "")).strip()
        if table_name:
            return table_name

    fallback = state.incident.job_name.strip()
    return fallback


def _build_accountability_report(state: AgentState) -> dict[str, Any]:
    diagnosis = state.diagnosis or {}
    blast_radius = state.blast_radius or {}
    detail = diagnosis.get("schema_drift_detail", {})
    detail_payload = detail if isinstance(detail, dict) else {}

    report = {
        "incident_id": state.incident.incident_id,
        "cluster": state.incident.cluster,
        "error_signature": state.incident.error_signature,
        "diagnosis_summary": diagnosis.get("summary", ""),
        "root_cause": diagnosis.get("root_cause", ""),
        "schema_drift_detail": detail_payload,
        "upstream_owner": blast_radius.get("upstream_owner", "team.unknown_owner"),
        "affected_paimon_tables": blast_radius.get("affected_paimon_tables", []),
        "affected_starrocks_dashboards": blast_radius.get("affected_starrocks_dashboards", []),
        "evidence": diagnosis.get("evidence", []),
    }
    return report


async def ingest_alert(state: AgentState) -> AgentState:
    try:
        state.context_bundle["normalized_incident"] = normalize_incident(state.incident)
        return state
    except Exception as exc:
        logger.exception("Failed to ingest alert")
        state.diagnosis_errors.append(str(exc))
        state.status = "failed"
        state.error_message = "failed to ingest alert"
        return state


async def retrieve_cases(state: AgentState) -> AgentState:
    try:
        query = _build_retrieval_query(state)
        if not query:
            state.retrieval_hits = []
            state.context_bundle["retrieval_hits"] = []
            return state

        state.retrieval_hits = hybrid_retrieve(
            query=query,
            incident=state.incident,
            top_k=5,
        )
        state.context_bundle["retrieval_hits"] = state.retrieval_hits
        return state
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to retrieve historical cases")
        state.diagnosis_errors.append(f"retrieve_cases_degraded: {exc}")
        state.retrieval_hits = []
        state.context_bundle["retrieval_hits"] = []
        return state


async def diagnose_rca(state: AgentState) -> AgentState:
    started_at = time.perf_counter()
    try:
        context = _build_incident_context(state)
        schema_drift_diagnosis = diagnose_schema_drift_incident(state.incident)

        try:
            diagnosis = analyze_root_cause_with_llm(
                log_snippets=state.incident.log_snippets,
                incident_context=context,
            )
            state.diagnosis = diagnosis.model_dump()
        except DiagnoseLLMServiceError as exc:
            logger.warning("LLM diagnosis unavailable, fallback to rule-based diagnosis")
            state.diagnosis_errors.append(f"llm_diagnosis_fallback: {exc}")
            state.diagnosis = diagnose_spark_incident(state.incident)

        if schema_drift_diagnosis is not None:
            base_diagnosis = state.diagnosis or {}
            state.diagnosis = _merge_schema_drift_diagnosis(base_diagnosis, schema_drift_diagnosis)
            detail = state.diagnosis.get("schema_drift_detail", {})
            if isinstance(detail, dict):
                state.context_bundle["schema_drift_detail"] = detail

        state.status = "diagnosed"
        return state
    except Exception as exc:
        logger.exception("Failed to diagnose incident")
        state.diagnosis_errors.append(str(exc))
        state.status = "failed"
        state.error_message = "failed to diagnose incident"
        return state
    finally:
        diagnosis_payload = state.diagnosis or {}
        incident_type = str(diagnosis_payload.get("incident_type", state.incident.incident_type.value))
        duration_seconds = time.perf_counter() - started_at
        observe_rca_duration_seconds(
            source=state.incident.source,
            incident_type=incident_type,
            duration_seconds=duration_seconds,
        )


async def plan_heal(state: AgentState) -> AgentState:
    try:
        diagnosis_payload = state.diagnosis

        if not diagnosis_payload:
            state.heal_plan = None
        else:
            context = _build_incident_context(state)
            try:
                diagnosis = RootCauseDiagnosis.model_validate(diagnosis_payload)
                heal_plan = generate_heal_plan_with_llm(
                    diagnosis=diagnosis,
                    incident_context=context,
                )
                state.heal_plan = heal_plan.model_dump()
            except PlannerLLMServiceError as exc:
                logger.warning("LLM planner unavailable, fallback to rule-based planner")
                state.diagnosis_errors.append(f"llm_planner_fallback: {exc}")
                state.heal_plan = build_heal_plan(state.incident, diagnosis_payload)
            except Exception:
                logger.warning("Diagnosis payload invalid for LLM planner, fallback to rule-based planner")
                state.heal_plan = build_heal_plan(state.incident, diagnosis_payload)

        if state.heal_plan is None:
            state.status = "escalated"
            state.error_message = "no recoverable heal plan available"

        return state
    except Exception as exc:
        logger.exception("Failed to build heal plan")
        state.diagnosis_errors.append(str(exc))
        state.status = "failed"
        state.error_message = "failed to build heal plan"
        return state


async def guardrail_check(state: AgentState) -> AgentState:
    try:
        state.guardrail_result = evaluate_guardrails(
            state.incident,
            state.diagnosis,
            state.heal_plan,
        )
        return state
    except Exception as exc:
        logger.exception("Failed to evaluate guardrails")
        state.diagnosis_errors.append(str(exc))
        state.status = "failed"
        state.error_message = "failed to evaluate guardrails"
        return state


async def human_approval(state: AgentState) -> AgentState:
    try:
        approval_result = evaluate_human_approval(state.incident, state.guardrail_result)

        if state.guardrail_result is None:
            state.status = "failed"
            state.error_message = "missing guardrail result"
            return state

        state.guardrail_result["approval_status"] = "approved" if approval_result["approved"] else "rejected"
        state.guardrail_result["approved_by"] = approval_result["approved_by"]
        state.guardrail_result["approval_reason"] = approval_result["reason"]

        if not approval_result["approved"]:
            state.status = "escalated"
            state.error_message = str(approval_result["reason"])

        return state
    except Exception as exc:
        logger.exception("Failed to evaluate approval")
        state.diagnosis_errors.append(str(exc))
        state.status = "failed"
        state.error_message = "failed to evaluate approval"
        return state


async def execute_heal(state: AgentState) -> AgentState:
    try:
        if state.retry_count >= state.max_retries:
            state.status = "failed"
            state.error_message = "max retries exceeded before execution"
            return state

        if not state.heal_plan:
            state.status = "escalated"
            state.error_message = "missing heal plan for execution"
            return state

        if not state.guardrail_result:
            state.status = "failed"
            state.error_message = "missing guardrail result for execution"
            return state

        record = await execute_controlled_retry(state.heal_plan)
        state.execution_records.append(record)
        state.retry_count += 1
        state.status = "diagnosed"
        return state
    except MCPToolCallError as exc:
        logger.warning("MCP tool invocation failed during heal execution")
        state.diagnosis_errors.append(f"mcp_execution_error: {exc}")
        state.status = "escalated"
        state.error_message = "heal execution degraded; manual escalation required"
        return state
    except Exception as exc:
        logger.exception("Failed to execute heal plan")
        state.diagnosis_errors.append(str(exc))
        state.status = "failed"
        state.error_message = "failed to execute heal plan"
        return state


async def verify_recovery(state: AgentState) -> AgentState:
    action_type = "unknown"
    try:
        if not state.heal_plan:
            state.status = "failed"
            state.error_message = "missing heal plan for recovery verification"
            return state

        action_type = str(state.heal_plan.get("action_type", "unknown"))

        target_id = str(state.heal_plan.get("target_id", "")).strip()
        if not target_id:
            state.status = "failed"
            state.error_message = "missing target id for recovery verification"
            return state

        verification_record = await verify_heal_recovery(target_id=target_id)
        state.execution_records.append(verification_record)

        if verification_record.get("recovered") is True:
            inc_action_success_total(action_type=action_type, success=True)
            state.status = "healed"
            state.error_message = ""
            return state

        inc_action_success_total(action_type=action_type, success=False)
        state.status = "escalated"
        state.error_message = "recovery verification failed; manual intervention required"
        return state
    except MCPToolCallError as exc:
        inc_action_success_total(action_type=action_type, success=False)
        logger.warning("MCP tool invocation failed during recovery verification")
        state.diagnosis_errors.append(f"mcp_execution_error: {exc}")
        state.status = "escalated"
        state.error_message = "recovery verification degraded; manual escalation required"
        return state
    except Exception as exc:  # noqa: BLE001
        inc_action_success_total(action_type=action_type, success=False)
        logger.exception("Failed to verify recovery")
        state.diagnosis_errors.append(str(exc))
        state.status = "escalated"
        state.error_message = "failed to verify recovery; manual escalation required"
        return state


async def controlled_retry(state: AgentState) -> AgentState:
    return await execute_heal(state)


async def circuit_break(state: AgentState) -> AgentState:
    try:
        if not state.diagnosis:
            state.status = "failed"
            state.error_message = "missing diagnosis for circuit break"
            return state

        if not bool(state.diagnosis.get("requires_circuit_break", False)):
            state.status = "failed"
            state.error_message = "circuit break requested without diagnosis signal"
            return state

        drifted_table_name = _collect_schema_drift_table_name(state)
        state.blast_radius = analyze_blast_radius(drifted_table_name)

        record = await execute_circuit_break(state.incident, state.blast_radius)
        state.execution_records.append(record)

        if record.get("result") == "isolated":
            state.status = "isolated"
            return state

        state.status = "escalated"
        state.error_message = "partial circuit break execution; manual follow-up required"
        return state
    except MCPToolCallError as exc:
        logger.warning("MCP tool invocation failed during circuit break")
        state.diagnosis_errors.append(f"mcp_execution_error: {exc}")
        state.status = "escalated"
        state.error_message = "circuit break degraded; manual escalation required"
        return state
    except Exception as exc:
        logger.exception("Failed to execute circuit break")
        state.diagnosis_errors.append(str(exc))
        state.status = "failed"
        state.error_message = "failed to execute circuit break"
        return state


async def notify_owners(state: AgentState) -> AgentState:
    try:
        report = _build_accountability_report(state)
        blast_radius = state.blast_radius or {}

        owner_candidates = [
            str(blast_radius.get("upstream_owner", "team.unknown_owner")).strip(),
            *[str(owner).strip() for owner in blast_radius.get("affected_dashboard_owners", [])],
        ]
        owners = [owner for owner in dict.fromkeys(owner_candidates) if owner]

        notification_targets: list[dict[str, str]] = []

        diagnosis = state.diagnosis or {}
        incident_label = str(diagnosis.get("incident_type", state.incident.incident_type.value))
        title = f"[NightWatch] Incident {state.incident.incident_id} ({incident_label})"

        body = (
            f"error_signature: {report['error_signature']}\n"
            f"root_cause: {report['root_cause']}\n"
            f"upstream_owner: {report['upstream_owner']}\n"
            f"affected_dashboards: {report['affected_starrocks_dashboards']}\n"
            f"evidence: {report['evidence']}"
        )

        for index, owner in enumerate(owners):
            system = "jira" if index == 0 else "github"
            notification_targets.append({"owner": owner, "system": system})

        tickets = await notify_accountability_owners(
            owners=owners,
            title=title,
            body=body,
            labels=[incident_label, state.incident.source, state.status],
        )

        state.notification_targets = notification_targets
        state.notification_payloads.append(report)
        state.execution_records.append(
            {
                "action_type": "notify_owners",
                "result": "created" if tickets else "skipped",
                "tickets": tickets,
            }
        )

        if not tickets:
            state.status = "escalated"
            if not state.error_message:
                state.error_message = "no owners resolved for accountability notification"
        elif state.status not in {"isolated", "escalated"}:
            state.status = "isolated"

        return state
    except MCPToolCallError as exc:
        logger.warning("MCP tool invocation failed during owner notification")
        state.diagnosis_errors.append(f"mcp_execution_error: {exc}")
        state.status = "escalated"
        state.error_message = "owner notification degraded; manual escalation required"
        return state
    except Exception as exc:
        logger.exception("Failed to notify owners")
        state.diagnosis_errors.append(str(exc))
        state.status = "failed"
        state.error_message = "failed to notify owners"
        return state