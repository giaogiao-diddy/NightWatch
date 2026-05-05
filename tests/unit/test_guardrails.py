from datetime import datetime

from nightwatch.healing.guardrails import evaluate_guardrails, evaluate_human_approval
from nightwatch.models.incident import IncidentEvent, IncidentType


def build_incident(incident_type: IncidentType, approval: str = "") -> IncidentEvent:
    labels = {"approval": approval} if approval else {}
    return IncidentEvent(
        incident_id="inc_guardrail_001",
        source="spark" if incident_type != IncidentType.FLINK_SCHEMA_DRIFT else "flink",
        incident_type=incident_type,
        severity="P0",
        cluster="dw-prod",
        job_name="critical_job",
        application_id="application_1714858800_1024",
        event_time=datetime(2026, 5, 4, 3, 0, 0),
        labels=labels,
    )


def test_evaluate_guardrails_blocks_excessive_executor_memory() -> None:
    incident = build_incident(IncidentType.SPARK_OOM)
    diagnosis = {
        "incident_type": IncidentType.SPARK_OOM.value,
        "risk_level": "medium",
        "recoverable": True,
        "requires_circuit_break": False,
    }
    heal_plan = {
        "action_type": "tune_and_retry",
        "target_system": "scheduler",
        "target_id": incident.job_name,
        "parameters": {"executor_memory_gb": 64, "shuffle_partitions": 800},
    }

    result = evaluate_guardrails(incident, diagnosis, heal_plan)

    assert result["decision"] == "block"
    assert "executor_memory_gb exceeds limit" in result["reason"]


def test_evaluate_guardrails_routes_high_risk_action_to_manual_approval() -> None:
    incident = build_incident(IncidentType.SPARK_SKEW)
    diagnosis = {
        "incident_type": IncidentType.SPARK_SKEW.value,
        "risk_level": "high",
        "recoverable": True,
        "requires_circuit_break": False,
    }
    heal_plan = {
        "action_type": "tune_and_retry",
        "target_system": "scheduler",
        "target_id": incident.job_name,
        "parameters": {"executor_memory_gb": 12, "shuffle_partitions": 1200},
    }

    result = evaluate_guardrails(incident, diagnosis, heal_plan)

    assert result["decision"] == "require_approval"
    assert result["approval_status"] == "pending"


def test_schema_drift_never_allows_automatic_retry_even_with_approval() -> None:
    incident = build_incident(IncidentType.FLINK_SCHEMA_DRIFT, approval="approved")
    diagnosis = {
        "incident_type": IncidentType.FLINK_SCHEMA_DRIFT.value,
        "risk_level": "critical",
        "recoverable": False,
        "requires_circuit_break": True,
    }
    heal_plan = {
        "action_type": "tune_and_retry",
        "target_system": "scheduler",
        "target_id": incident.job_name,
        "parameters": {"executor_memory_gb": 8, "shuffle_partitions": 400},
    }

    guardrail_result = evaluate_guardrails(incident, diagnosis, heal_plan)
    approval_result = evaluate_human_approval(incident, guardrail_result)

    assert guardrail_result["decision"] == "block"
    assert "schema drift" in guardrail_result["reason"].lower()
    assert approval_result["approved"] is False