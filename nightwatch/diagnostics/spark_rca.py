import logging
import time
from typing import Any

from openai import OpenAI

try:
    import instructor
except ImportError:  # pragma: no cover - import guard for runtime environment
    instructor = None  # type: ignore[assignment]

from nightwatch.agent.prompts import SYSTEM_DIAGNOSE_INCIDENT
from nightwatch.config import get_settings
from nightwatch.models.api import RootCauseDiagnosis
from nightwatch.models.incident import IncidentEvent, IncidentType, LogSnippet


logger = logging.getLogger(__name__)


OOM_MARKERS = (
    "outofmemoryerror",
    "exceeding memory limits",
    "executorlostfailure",
    "container killed by yarn",
    "java heap space",
)

SKEW_MARKERS = (
    "skewed partition",
    "data skew",
    "too large partition",
    "imbalanced shuffle",
)

DRIVER_MARKERS = (
    "driver is up but is not responsive",
    "gc overhead limit exceeded",
    "driver contention",
)


class LLMServiceError(RuntimeError):
    pass


def _build_diagnosis_user_prompt(
    snippets: list[LogSnippet],
    incident_context: dict[str, Any] | None,
    max_prompt_chars: int,
) -> str:
    context = incident_context or {}
    retrieval_hints = context.get("retrieval_hints", [])

    header_lines = [
        "请基于以下上下文进行结构化 RCA：",
        f"incident_id: {context.get('incident_id', '')}",
        f"source: {context.get('source', '')}",
        f"cluster: {context.get('cluster', '')}",
        f"job_name: {context.get('job_name', '')}",
        f"application_id: {context.get('application_id', '')}",
        f"event_time: {context.get('event_time', '')}",
        f"error_signature: {context.get('error_signature', '')}",
    ]

    if isinstance(retrieval_hints, list) and retrieval_hints:
        header_lines.append("历史案例召回（仅供辅助，不可替代日志证据）：")
        for hint in retrieval_hints[:3]:
            if not isinstance(hint, dict):
                continue
            header_lines.append(
                "- "
                f"case_id={hint.get('case_id', '')}; "
                f"root_cause={hint.get('root_cause', '')}; "
                f"recommended_action={hint.get('recommended_action', '')}; "
                f"score={hint.get('score', 0.0)}"
            )

    header_lines.extend(["", "日志切片（按相关性输入）："])

    body_lines: list[str] = []
    for index, snippet in enumerate(snippets, start=1):
        body_lines.append(
            (
                f"[Snippet {index}] source={snippet.source} host={snippet.host} "
                f"offset={snippet.offset_start}-{snippet.offset_end} score={snippet.score}\n"
                f"{snippet.content.strip()}"
            )
        )

    user_prompt = "\n".join(header_lines + body_lines).strip()
    if len(user_prompt) <= max_prompt_chars:
        return user_prompt

    truncated = user_prompt[:max_prompt_chars]
    last_newline = truncated.rfind("\n")
    if last_newline > 0:
        truncated = truncated[:last_newline]
    return f"{truncated}\n[TRUNCATED]"


def _build_instructor_client() -> Any:
    if instructor is None:
        raise LLMServiceError(
            "instructor is not installed; please install instructor before using LLM diagnosis"
        )

    settings = get_settings()
    if not settings.llm_api_key:
        raise LLMServiceError("missing llm_api_key for LLM diagnosis")

    openai_client = OpenAI(
        api_key=settings.llm_api_key,
        base_url=settings.llm_base_url,
        timeout=settings.llm_timeout_seconds,
    )
    return instructor.from_openai(openai_client)


def analyze_root_cause_with_llm(
    log_snippets: list[LogSnippet],
    incident_context: dict[str, Any] | None = None,
    client: Any | None = None,
) -> RootCauseDiagnosis:
    if not log_snippets:
        raise ValueError("log_snippets cannot be empty for llm diagnosis")

    settings = get_settings()
    llm_client = client or _build_instructor_client()
    user_prompt = _build_diagnosis_user_prompt(
        snippets=log_snippets,
        incident_context=incident_context,
        max_prompt_chars=settings.llm_max_prompt_chars,
    )

    last_error: Exception | None = None
    for attempt in range(1, settings.llm_max_retries + 1):
        try:
            response = llm_client.chat.completions.create(
                model=settings.llm_model,
                response_model=RootCauseDiagnosis,
                temperature=settings.llm_temperature,
                messages=[
                    {"role": "system", "content": SYSTEM_DIAGNOSE_INCIDENT},
                    {"role": "user", "content": user_prompt},
                ],
            )
            if not isinstance(response, RootCauseDiagnosis):
                raise LLMServiceError("LLM diagnosis response is not RootCauseDiagnosis")
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.warning(
                "LLM diagnosis attempt failed",
                extra={"attempt": attempt, "max_retries": settings.llm_max_retries},
            )
            if attempt < settings.llm_max_retries:
                time.sleep(min(0.2 * attempt, 1.0))

    raise LLMServiceError("failed to analyze root cause with llm") from last_error


def _collect_incident_text(incident: IncidentEvent) -> str:
    parts = [incident.error_signature]
    parts.extend(snippet.content for snippet in incident.log_snippets)
    return "\n".join(part for part in parts if part).lower()


def _collect_evidence(incident: IncidentEvent, markers: tuple[str, ...]) -> list[str]:
    evidence: list[str] = []
    for snippet in incident.log_snippets:
        content_lower = snippet.content.lower()
        if any(marker in content_lower for marker in markers):
            evidence.append(snippet.content)
    if not evidence and incident.error_signature:
        evidence.append(incident.error_signature)
    return evidence or ["no direct evidence captured"]


def diagnose_spark_incident(incident: IncidentEvent) -> dict[str, object]:
    incident_text = _collect_incident_text(incident)

    if any(marker in incident_text for marker in SKEW_MARKERS) or incident.incident_type is IncidentType.SPARK_SKEW:
        return {
            "incident_type": IncidentType.SPARK_SKEW.value,
            "summary": "Spark skew pattern detected",
            "root_cause": "Shuffle stage contains oversized or skewed partitions",
            "confidence": 0.95,
            "risk_level": "high",
            "recoverable": True,
            "requires_circuit_break": False,
            "evidence": _collect_evidence(incident, SKEW_MARKERS),
        }

    if any(marker in incident_text for marker in DRIVER_MARKERS) or incident.incident_type is IncidentType.DRIVER_CONTENTION:
        return {
            "incident_type": IncidentType.DRIVER_CONTENTION.value,
            "summary": "Spark driver contention detected",
            "root_cause": "Driver side resources are saturated or blocked by GC contention",
            "confidence": 0.88,
            "risk_level": "high",
            "recoverable": True,
            "requires_circuit_break": False,
            "evidence": _collect_evidence(incident, DRIVER_MARKERS),
        }

    if any(marker in incident_text for marker in OOM_MARKERS) or incident.incident_type is IncidentType.SPARK_OOM:
        return {
            "incident_type": IncidentType.SPARK_OOM.value,
            "summary": "Spark executor OOM detected",
            "root_cause": "Executor memory exhausted during batch processing",
            "confidence": 0.92,
            "risk_level": "medium",
            "recoverable": True,
            "requires_circuit_break": False,
            "evidence": _collect_evidence(incident, OOM_MARKERS),
        }

    return {
        "incident_type": incident.incident_type.value,
        "summary": "Unknown incident",
        "root_cause": "No mock RCA strategy available",
        "confidence": 0.2,
        "risk_level": "high",
        "recoverable": False,
        "requires_circuit_break": False,
        "evidence": _collect_evidence(incident, tuple()),
    }