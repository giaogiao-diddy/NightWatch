from datetime import datetime, timedelta

from nightwatch.ingest.correlator import correlate_incidents
from nightwatch.models.incident import IncidentEvent, IncidentType, LogSnippet


def build_event(index: int, base_time: datetime, application_id: str, job_name: str) -> IncidentEvent:
    return IncidentEvent(
        incident_id=f"inc_{index}",
        source="spark",
        incident_type=IncidentType.SPARK_OOM,
        severity="P0",
        cluster="dw-prod",
        job_name=job_name,
        application_id=application_id,
        event_time=base_time + timedelta(seconds=index * 5),
        error_signature=f"executor oom {index}",
        labels={"source_alert_id": str(index)},
        log_snippets=[
            LogSnippet(
                source="yarn",
                host="worker-01",
                offset_start=index,
                offset_end=index + 1,
                content=f"ExecutorLostFailure fragment {index}",
                score=1.0,
            )
        ],
    )


def test_correlate_incidents_folds_five_concurrent_same_source_alerts_into_one() -> None:
    base_time = datetime(2026, 5, 4, 3, 0, 0)
    events = [
        build_event(index=index, base_time=base_time, application_id="application_001", job_name="dws_job")
        for index in range(5)
    ]

    correlated = correlate_incidents(events, window_seconds=60)

    assert len(correlated) == 1
    merged = correlated[0]
    assert merged.application_id == "application_001"
    assert merged.job_name == "dws_job"
    assert merged.labels["correlated_event_count"] == "5"
    assert len(merged.log_snippets) == 5
    assert merged.event_time == base_time


def test_correlate_incidents_keeps_same_source_alerts_separate_outside_window() -> None:
    base_time = datetime(2026, 5, 4, 3, 0, 0)
    first = build_event(index=0, base_time=base_time, application_id="application_002", job_name="dws_job")
    second = IncidentEvent(
        incident_id="inc_late",
        source="spark",
        incident_type=IncidentType.SPARK_OOM,
        severity="P0",
        cluster="dw-prod",
        job_name="dws_job",
        application_id="application_002",
        event_time=base_time + timedelta(seconds=180),
        error_signature="executor oom late",
    )

    correlated = correlate_incidents([first, second], window_seconds=60)

    assert len(correlated) == 2
    assert correlated[0].labels["correlated_event_count"] == "1"
    assert correlated[1].labels["correlated_event_count"] == "1"