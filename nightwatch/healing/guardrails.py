from nightwatch.agent.policies import (
    APPROVAL_LABEL_KEY,
    APPROVED_VALUES,
    CONTROLLED_RETRY_ACTION,
    GUARDRAIL_ALLOW,
    GUARDRAIL_BLOCK,
    GUARDRAIL_REQUIRE_APPROVAL,
    MAX_EXECUTOR_MEMORY_GB,
    MAX_SHUFFLE_PARTITIONS,
    MOCK_APPROVER,
    TUNE_AND_RETRY_ACTION,
)
from nightwatch.models.incident import IncidentEvent, IncidentType


def evaluate_guardrails(
    incident: IncidentEvent,
    diagnosis: dict[str, object] | None,
    heal_plan: dict[str, object] | None,
) -> dict[str, object]:
    if not diagnosis or not heal_plan:
        return {
            "decision": GUARDRAIL_BLOCK,
            "reason": "missing diagnosis or heal plan",
            "approval_status": "not_applicable",
        }

    action_type = str(heal_plan.get("action_type", ""))
    diagnosis_type = str(diagnosis.get("incident_type", incident.incident_type.value))
    parameters = heal_plan.get("parameters", {})

    if not isinstance(parameters, dict):
        return {
            "decision": GUARDRAIL_BLOCK,
            "reason": "invalid heal plan parameters",
            "approval_status": "not_applicable",
        }

    if action_type == TUNE_AND_RETRY_ACTION and diagnosis_type == IncidentType.FLINK_SCHEMA_DRIFT.value:
        return {
            "decision": GUARDRAIL_BLOCK,
            "reason": "schema drift incidents cannot use automatic tune_and_retry",
            "approval_status": "not_applicable",
        }

    executor_memory_gb = parameters.get("executor_memory_gb")
    if isinstance(executor_memory_gb, int) and executor_memory_gb > MAX_EXECUTOR_MEMORY_GB:
        return {
            "decision": GUARDRAIL_BLOCK,
            "reason": f"executor_memory_gb exceeds limit {MAX_EXECUTOR_MEMORY_GB}",
            "approval_status": "not_applicable",
        }

    shuffle_partitions = parameters.get("shuffle_partitions")
    if isinstance(shuffle_partitions, int) and shuffle_partitions > MAX_SHUFFLE_PARTITIONS:
        return {
            "decision": GUARDRAIL_BLOCK,
            "reason": f"shuffle_partitions exceeds limit {MAX_SHUFFLE_PARTITIONS}",
            "approval_status": "not_applicable",
        }

    if bool(diagnosis.get("requires_circuit_break", False)):
        return {
            "decision": GUARDRAIL_BLOCK,
            "reason": "circuit break required; automatic retry is forbidden",
            "approval_status": "not_applicable",
        }

    if str(diagnosis.get("risk_level", "")).lower() in {"high", "critical"}:
        return {
            "decision": GUARDRAIL_REQUIRE_APPROVAL,
            "reason": "high-risk operation requires human approval",
            "approval_status": "pending",
        }

    return {
        "decision": GUARDRAIL_ALLOW,
        "reason": "guardrails passed",
        "approval_status": "not_required",
    }


def evaluate_human_approval(
    incident: IncidentEvent,
    guardrail_result: dict[str, object] | None,
) -> dict[str, object]:
    if not guardrail_result:
        return {
            "approved": False,
            "approved_by": "",
            "reason": "missing guardrail result",
        }

    if guardrail_result.get("decision") == GUARDRAIL_BLOCK:
        return {
            "approved": False,
            "approved_by": "",
            "reason": str(guardrail_result.get("reason", "guardrail blocked operation")),
        }

    approval_value = incident.labels.get(APPROVAL_LABEL_KEY, "").lower()
    if approval_value in APPROVED_VALUES:
        return {
            "approved": True,
            "approved_by": MOCK_APPROVER,
            "reason": "mock approval granted",
        }

    return {
        "approved": False,
        "approved_by": "",
        "reason": "mock approval not granted",
    }