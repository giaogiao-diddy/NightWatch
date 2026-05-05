from collections import defaultdict
from datetime import timedelta

from nightwatch.models.incident import IncidentEvent, IncidentType, LogSnippet, MetricSnapshot


SEVERITY_ORDER = {
    "P0": 0,
    "P1": 1,
    "P2": 2,
    "P3": 3,
}


def build_correlation_key(event: IncidentEvent) -> str:
    if event.application_id:
        return f"application:{event.application_id}"
    if event.job_name:
        return f"job:{event.job_name}"
    return f"incident:{event.incident_id}"


def select_primary_incident(events: list[IncidentEvent]) -> IncidentEvent:
    return min(
        events,
        key=lambda event: (
            SEVERITY_ORDER.get(event.severity.upper(), 99),
            event.event_time,
        ),
    )


def merge_log_snippets(events: list[IncidentEvent]) -> list[LogSnippet]:
    merged: list[LogSnippet] = []
    seen: set[tuple[str, int, int, str]] = set()
    for event in events:
        for snippet in event.log_snippets:
            key = (snippet.source, snippet.offset_start, snippet.offset_end, snippet.content)
            if key in seen:
                continue
            seen.add(key)
            merged.append(snippet)
    return merged


def merge_metrics(events: list[IncidentEvent]) -> list[MetricSnapshot]:
    merged: list[MetricSnapshot] = []
    seen: set[tuple[str, str, float]] = set()
    for event in events:
        for metric in event.metrics:
            key = (metric.name, metric.ts.isoformat(), metric.value)
            if key in seen:
                continue
            seen.add(key)
            merged.append(metric)
    return merged


def merge_incident_group(events: list[IncidentEvent]) -> IncidentEvent:
    if not events:
        raise ValueError("cannot merge an empty incident group")

    ordered_events = sorted(events, key=lambda event: event.event_time)
    primary = select_primary_incident(ordered_events)
    merged_labels: dict[str, str] = {}
    for event in ordered_events:
        merged_labels.update(event.labels)
    merged_labels["correlated_event_count"] = str(len(ordered_events))

    incident_type = next(
        (event.incident_type for event in ordered_events if event.incident_type is not IncidentType.UNKNOWN),
        primary.incident_type,
    )

    return IncidentEvent(
        incident_id=primary.incident_id,
        source=primary.source,
        incident_type=incident_type,
        severity=primary.severity,
        cluster=primary.cluster,
        job_name=primary.job_name,
        application_id=primary.application_id,
        scheduler_task_id=primary.scheduler_task_id,
        event_time=ordered_events[0].event_time,
        error_signature=primary.error_signature,
        labels=merged_labels,
        log_snippets=merge_log_snippets(ordered_events),
        metrics=merge_metrics(ordered_events),
    )


def correlate_incidents(
    events: list[IncidentEvent],
    window_seconds: int = 120,
) -> list[IncidentEvent]:
    if window_seconds < 0:
        raise ValueError("window_seconds cannot be negative")
    if not events:
        return []

    ordered_events = sorted(events, key=lambda event: event.event_time)
    active_groups: dict[str, list[IncidentEvent]] = defaultdict(list)
    merged_events: list[IncidentEvent] = []

    for event in ordered_events:
        key = build_correlation_key(event)
        group = active_groups[key]

        if not group:
            group.append(event)
            continue

        last_event = group[-1]
        if event.event_time - last_event.event_time <= timedelta(seconds=window_seconds):
            group.append(event)
            continue

        merged_events.append(merge_incident_group(group))
        active_groups[key] = [event]

    for group in active_groups.values():
        if group:
            merged_events.append(merge_incident_group(group))

    return sorted(merged_events, key=lambda event: event.event_time)