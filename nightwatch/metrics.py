import logging
from typing import Any

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, REGISTRY, generate_latest
except ImportError:  # pragma: no cover - optional runtime dependency
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    Counter = None  # type: ignore[assignment]
    Histogram = None  # type: ignore[assignment]
    REGISTRY = None  # type: ignore[assignment]
    generate_latest = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


def _get_existing_collector(name: str) -> Any | None:
    names_to_collectors = getattr(REGISTRY, "_names_to_collectors", None)
    if not isinstance(names_to_collectors, dict):
        return None
    return names_to_collectors.get(name)


def _build_counter(name: str, description: str, labels: tuple[str, ...]):
    if Counter is None:
        return None

    try:
        return Counter(name, description, labels)
    except ValueError:
        return _get_existing_collector(name)


def _build_histogram(name: str, description: str, labels: tuple[str, ...]):
    if Histogram is None:
        return None

    try:
        return Histogram(name, description, labels)
    except ValueError:
        return _get_existing_collector(name)


INCIDENTS_TOTAL = _build_counter(
    name="nightwatch_incidents_total",
    description="Total incidents ingested by NightWatch",
    labels=("source",),
)

RCA_DURATION_SECONDS = _build_histogram(
    name="nightwatch_rca_duration_seconds",
    description="RCA diagnosis latency distribution in seconds",
    labels=("source", "incident_type"),
)

ACTION_SUCCESS_TOTAL = _build_counter(
    name="nightwatch_action_success_total",
    description="Outcome of automated remediation validation based on recovery verification",
    labels=("action_type", "result"),
)


def inc_incidents_total(source: str) -> None:
    if INCIDENTS_TOTAL is None:
        return

    try:
        INCIDENTS_TOTAL.labels(source=source or "unknown").inc()
    except Exception:  # noqa: BLE001
        logger.exception("failed to increase incident total metric")


def observe_rca_duration_seconds(source: str, incident_type: str, duration_seconds: float) -> None:
    if RCA_DURATION_SECONDS is None:
        return

    safe_duration = max(duration_seconds, 0.0)
    try:
        RCA_DURATION_SECONDS.labels(
            source=source or "unknown",
            incident_type=incident_type or "unknown",
        ).observe(safe_duration)
    except Exception:  # noqa: BLE001
        logger.exception("failed to observe rca duration metric")


def inc_action_success_total(action_type: str, success: bool) -> None:
    if ACTION_SUCCESS_TOTAL is None:
        return

    result = "success" if success else "failure"
    try:
        ACTION_SUCCESS_TOTAL.labels(action_type=action_type or "unknown", result=result).inc()
    except Exception:  # noqa: BLE001
        logger.exception("failed to increase action success metric")


def render_metrics_payload() -> tuple[bytes, str]:
    if generate_latest is None:
        return b"", CONTENT_TYPE_LATEST

    try:
        return generate_latest(), CONTENT_TYPE_LATEST
    except Exception:  # noqa: BLE001
        logger.exception("failed to render prometheus metrics payload")
        return b"", CONTENT_TYPE_LATEST