import logging
import time
from typing import Any

from openai import OpenAI

try:
    import instructor
except ImportError:  # pragma: no cover - import guard for runtime environment
    instructor = None  # type: ignore[assignment]

from nightwatch.agent.policies import RESTART_ACTION, TUNE_AND_RETRY_ACTION
from nightwatch.agent.prompts import SYSTEM_PLAN_HEAL
from nightwatch.config import get_settings
from nightwatch.models.api import HealActionPlan, RootCauseDiagnosis
from nightwatch.models.incident import IncidentEvent, IncidentType


logger = logging.getLogger(__name__)


class LLMServiceError(RuntimeError):
    pass


def _build_planner_user_prompt(
    diagnosis: RootCauseDiagnosis,
    incident_context: dict[str, Any] | None,
    max_prompt_chars: int,
) -> str:
    context = incident_context or {}
    content = "\n".join(
        [
            "请根据诊断结果生成单一可执行修复动作：",
            f"incident_id: {context.get('incident_id', '')}",
            f"source: {context.get('source', '')}",
            f"cluster: {context.get('cluster', '')}",
            f"job_name: {context.get('job_name', '')}",
            f"application_id: {context.get('application_id', '')}",
            "",
            f"diagnosis.incident_type: {diagnosis.incident_type}",
            f"diagnosis.summary: {diagnosis.summary}",
            f"diagnosis.root_cause: {diagnosis.root_cause}",
            f"diagnosis.confidence: {diagnosis.confidence}",
            f"diagnosis.risk_level: {diagnosis.risk_level}",
            f"diagnosis.recoverable: {diagnosis.recoverable}",
            f"diagnosis.requires_circuit_break: {diagnosis.requires_circuit_break}",
            "diagnosis.evidence:",
            *[f"- {item}" for item in diagnosis.evidence],
        ]
    ).strip()

    if len(content) <= max_prompt_chars:
        return content
    truncated = content[:max_prompt_chars]
    last_newline = truncated.rfind("\n")
    if last_newline > 0:
        truncated = truncated[:last_newline]
    return f"{truncated}\n[TRUNCATED]"


def _build_instructor_client() -> Any:
    if instructor is None:
        raise LLMServiceError(
            "instructor is not installed; please install instructor before using LLM planner"
        )

    settings = get_settings()
    if not settings.llm_api_key:
        raise LLMServiceError("missing llm_api_key for LLM planner")

    openai_client = OpenAI(
        api_key=settings.llm_api_key,
        base_url=settings.llm_base_url,
        timeout=settings.llm_timeout_seconds,
    )
    return instructor.from_openai(openai_client)


def generate_heal_plan_with_llm(
    diagnosis: RootCauseDiagnosis,
    incident_context: dict[str, Any] | None = None,
    client: Any | None = None,
) -> HealActionPlan:
    settings = get_settings()
    llm_client = client or _build_instructor_client()
    user_prompt = _build_planner_user_prompt(
        diagnosis=diagnosis,
        incident_context=incident_context,
        max_prompt_chars=settings.llm_max_prompt_chars,
    )

    last_error: Exception | None = None
    for attempt in range(1, settings.llm_max_retries + 1):
        try:
            response = llm_client.chat.completions.create(
                model=settings.llm_model,
                response_model=HealActionPlan,
                temperature=settings.llm_temperature,
                messages=[
                    {"role": "system", "content": SYSTEM_PLAN_HEAL},
                    {"role": "user", "content": user_prompt},
                ],
            )
            if not isinstance(response, HealActionPlan):
                raise LLMServiceError("LLM heal plan response is not HealActionPlan")
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.warning(
                "LLM planner attempt failed",
                extra={"attempt": attempt, "max_retries": settings.llm_max_retries},
            )
            if attempt < settings.llm_max_retries:
                time.sleep(min(0.2 * attempt, 1.0))

    raise LLMServiceError("failed to generate heal plan with llm") from last_error


def build_heal_plan(
    incident: IncidentEvent,
    diagnosis: dict[str, object] | None,
) -> dict[str, object] | None:
    if not diagnosis:
        return None

    diagnosis_type = str(diagnosis.get("incident_type", incident.incident_type.value))
    recoverable = bool(diagnosis.get("recoverable", False))

    if not recoverable:
        return None

    if diagnosis_type == IncidentType.SPARK_SKEW.value:
        return {
            "action_type": TUNE_AND_RETRY_ACTION,
            "target_system": "scheduler",
            "target_id": incident.job_name,
            "reason": "mock_plan_for_spark_skew",
            "parameters": {
                "executor_memory_gb": 12,
                "shuffle_partitions": 1200,
                "skew_optimization": "salting",
            },
        }

    if diagnosis_type == IncidentType.SPARK_OOM.value:
        return {
            "action_type": TUNE_AND_RETRY_ACTION,
            "target_system": "scheduler",
            "target_id": incident.job_name,
            "reason": "mock_plan_for_spark_oom",
            "parameters": {
                "executor_memory_gb": 10,
                "shuffle_partitions": 600,
            },
        }

    return {
        "action_type": RESTART_ACTION,
        "target_system": "scheduler",
        "target_id": incident.job_name,
        "reason": "mock_restart_for_unknown_recoverable_incident",
        "parameters": {},
    }