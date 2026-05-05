import re
from typing import Any

from nightwatch.models.incident import IncidentEvent, IncidentType, LogSnippet


SCHEMA_DRIFT_MARKERS = (
    "unsupported cdc data type",
    "schema drift",
    "column type mismatch",
    "ddl changed",
    "cannot deserialize",
)

TABLE_PATTERN = re.compile(
    r"(?:table|source table|upstream table)\s*[=:]\s*[`\"]?([a-zA-Z0-9_.-]+)[`\"]?",
    re.IGNORECASE,
)
COLUMN_PATTERN = re.compile(
    r"(?:column|field)\s*[=:]\s*[`\"]?([a-zA-Z0-9_.-]+)[`\"]?",
    re.IGNORECASE,
)
TABLE_COLUMN_PATTERN = re.compile(
    r"([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
    re.IGNORECASE,
)
TYPE_CHANGE_PATTERN = re.compile(
    r"(?:from|old[_ ]?type\s*[=:])\s*([a-zA-Z0-9_(),.<>]+).*?(?:to|new[_ ]?type\s*[=:])\s*([a-zA-Z0-9_(),.<>]+)",
    re.IGNORECASE,
)


def _contains_schema_drift_signal(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in SCHEMA_DRIFT_MARKERS)


def _extract_table_and_column(text: str) -> tuple[str, str]:
    table_match = TABLE_PATTERN.search(text)
    column_match = COLUMN_PATTERN.search(text)

    table_name = table_match.group(1) if table_match else ""
    column_name = column_match.group(1) if column_match else ""

    if table_name and column_name:
        return table_name, column_name

    table_column_match = TABLE_COLUMN_PATTERN.search(text)
    if table_column_match:
        table_name = table_name or table_column_match.group(1)
        column_name = column_name or table_column_match.group(2)

    return table_name, column_name


def _extract_type_change(text: str) -> tuple[str, str]:
    match = TYPE_CHANGE_PATTERN.search(text)
    if not match:
        return "", ""
    return match.group(1), match.group(2)


def extract_schema_drift_details(
    log_snippets: list[LogSnippet],
    error_signature: str = "",
) -> dict[str, Any] | None:
    if not log_snippets and not error_signature:
        return None

    candidate_texts: list[str] = []
    if error_signature:
        candidate_texts.append(error_signature)
    candidate_texts.extend(snippet.content for snippet in log_snippets if snippet.content)

    evidence: list[str] = []
    table_name = ""
    column_name = ""
    old_type = ""
    new_type = ""

    for text in candidate_texts:
        if not _contains_schema_drift_signal(text):
            continue
        evidence.append(text)
        if not table_name or not column_name:
            extracted_table, extracted_column = _extract_table_and_column(text)
            table_name = table_name or extracted_table
            column_name = column_name or extracted_column
        if not old_type or not new_type:
            extracted_old_type, extracted_new_type = _extract_type_change(text)
            old_type = old_type or extracted_old_type
            new_type = new_type or extracted_new_type

    if not evidence:
        return None

    return {
        "table_name": table_name,
        "column_name": column_name,
        "old_type": old_type,
        "new_type": new_type,
        "evidence": evidence[:3],
    }


def diagnose_schema_drift_incident(incident: IncidentEvent) -> dict[str, Any] | None:
    details = extract_schema_drift_details(
        log_snippets=incident.log_snippets,
        error_signature=incident.error_signature,
    )

    if incident.incident_type is not IncidentType.FLINK_SCHEMA_DRIFT and details is None:
        return None

    details = details or {
        "table_name": "",
        "column_name": "",
        "old_type": "",
        "new_type": "",
        "evidence": [incident.error_signature or "schema drift signal detected"],
    }

    table_name = details.get("table_name", "")
    column_name = details.get("column_name", "")
    old_type = details.get("old_type", "")
    new_type = details.get("new_type", "")

    summary = "Flink CDC schema drift detected"
    root_cause = "Upstream schema changed and broke CDC compatibility"

    if table_name and column_name:
        root_cause = f"Schema drift on {table_name}.{column_name} caused CDC deserialization failure"
    if old_type and new_type:
        root_cause = f"Schema drift from {old_type} to {new_type} broke CDC compatibility"

    return {
        "incident_type": IncidentType.FLINK_SCHEMA_DRIFT.value,
        "summary": summary,
        "root_cause": root_cause,
        "confidence": 0.98,
        "risk_level": "critical",
        "recoverable": False,
        "requires_circuit_break": True,
        "evidence": details["evidence"],
        "schema_drift_detail": details,
    }
