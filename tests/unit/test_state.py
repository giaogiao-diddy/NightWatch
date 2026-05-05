from datetime import datetime

import pytest
from pydantic import ValidationError

from nightwatch.models.incident import IncidentEvent, IncidentType
from nightwatch.models.state import AgentState


def build_incident() -> IncidentEvent:
    return IncidentEvent(
        incident_id="inc_test_001",
        source="spark",
        incident_type=IncidentType.SPARK_OOM,
        severity="P0",
        cluster="dw-prod",
        job_name="dws_user_growth_nightly",
        application_id="application_1714858800_1024",
        event_time=datetime(2026, 5, 4, 3, 0, 0),
        error_signature="Container killed by YARN for exceeding memory limits",
    )


def test_agent_state_initializes_with_expected_defaults() -> None:
    state = AgentState(incident=build_incident())

    assert state.mode == "assist"
    assert state.status == "pending"
    assert state.retry_count == 0
    assert state.max_retries == 2
    assert state.correlated_events == []
    assert state.context_bundle == {}
    assert state.execution_records == []
    assert state.incident.incident_type is IncidentType.SPARK_OOM


def test_agent_state_rejects_invalid_mode() -> None:
    with pytest.raises(ValidationError):
        AgentState(incident=build_incident(), mode="manual")