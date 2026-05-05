from datetime import datetime

from nightwatch.diagnostics.spark_rca import diagnose_spark_incident
from nightwatch.models.incident import IncidentEvent, IncidentType, LogSnippet


def build_incident(*log_messages: str, error_signature: str = "") -> IncidentEvent:
    snippets = [
        LogSnippet(
            source="yarn",
            host="worker-01",
            offset_start=index,
            offset_end=index + 1,
            content=message,
        )
        for index, message in enumerate(log_messages)
    ]
    return IncidentEvent(
        incident_id="inc_unit_spark",
        source="spark",
        incident_type=IncidentType.UNKNOWN,
        severity="P0",
        cluster="dw-prod",
        job_name="dws_user_growth_nightly",
        application_id="application_1714858800_1024",
        event_time=datetime(2026, 5, 4, 3, 0, 0),
        error_signature=error_signature,
        log_snippets=snippets,
    )


def test_diagnose_spark_incident_classifies_executor_oom() -> None:
    incident = build_incident(
        "ExecutorLostFailure: Container killed by YARN for exceeding memory limits.",
        error_signature="java.lang.OutOfMemoryError: Java heap space",
    )

    diagnosis = diagnose_spark_incident(incident)

    assert diagnosis["incident_type"] == IncidentType.SPARK_OOM.value
    assert diagnosis["recoverable"] is True
    assert diagnosis["risk_level"] == "medium"
    assert any("memory" in evidence.lower() for evidence in diagnosis["evidence"])


def test_diagnose_spark_incident_classifies_data_skew() -> None:
    incident = build_incident(
        "Detected skewed partition 42 with size 6.2GB during shuffle stage.",
        "Data skew causes one reducer to process most records.",
    )

    diagnosis = diagnose_spark_incident(incident)

    assert diagnosis["incident_type"] == IncidentType.SPARK_SKEW.value
    assert diagnosis["recoverable"] is True
    assert diagnosis["risk_level"] == "high"
    assert any("skew" in evidence.lower() for evidence in diagnosis["evidence"])