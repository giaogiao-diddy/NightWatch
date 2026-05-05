from datetime import datetime
from unittest.mock import MagicMock

from nightwatch.diagnostics.spark_rca import analyze_root_cause_with_llm
from nightwatch.healing.planner import generate_heal_plan_with_llm
from nightwatch.models.api import HealActionPlan, RootCauseDiagnosis
from nightwatch.models.incident import LogSnippet


def test_analyze_root_cause_with_llm_deserializes_structured_response() -> None:
    snippet = LogSnippet(
        source="yarn",
        host="worker-01",
        offset_start=100,
        offset_end=130,
        content="ExecutorLostFailure: Container killed by YARN for exceeding memory limits",
        score=0.96,
    )

    expected = RootCauseDiagnosis(
        incident_type="spark_oom",
        summary="Spark executor memory exhausted",
        root_cause="Executor heap cannot hold shuffle workload",
        confidence=0.93,
        evidence=["ExecutorLostFailure: Container killed by YARN for exceeding memory limits"],
        risk_level="high",
        recoverable=True,
        requires_circuit_break=False,
    )

    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = expected

    result = analyze_root_cause_with_llm(
        log_snippets=[snippet],
        incident_context={
            "incident_id": "inc_llm_001",
            "source": "spark",
            "cluster": "dw-prod",
            "job_name": "dws_user_growth_nightly",
            "application_id": "application_001",
            "event_time": datetime(2026, 5, 4, 3, 0, 0).isoformat(),
            "error_signature": "java.lang.OutOfMemoryError: Java heap space",
        },
        client=mock_client,
    )

    assert isinstance(result, RootCauseDiagnosis)
    assert result.incident_type == "spark_oom"
    assert result.recoverable is True
    assert result.confidence == 0.93

    call_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert call_kwargs["response_model"] is RootCauseDiagnosis
    assert call_kwargs["messages"][0]["role"] == "system"
    assert call_kwargs["messages"][1]["role"] == "user"


def test_generate_heal_plan_with_llm_returns_structured_action_plan() -> None:
    diagnosis = RootCauseDiagnosis(
        incident_type="spark_oom",
        summary="Spark executor memory exhausted",
        root_cause="Executor heap cannot hold shuffle workload",
        confidence=0.9,
        evidence=["OutOfMemoryError"],
        risk_level="medium",
        recoverable=True,
        requires_circuit_break=False,
    )

    expected_plan = HealActionPlan(
        action_type="tune_and_retry",
        target_system="scheduler",
        target_id="dws_user_growth_nightly",
        parameters={
            "executor_memory_gb": 10,
            "shuffle_partitions": 800,
        },
        reason="Increase executor memory and rebalance partitions before retry",
        cooldown_seconds=60,
        requires_approval=True,
    )

    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = expected_plan

    result = generate_heal_plan_with_llm(
        diagnosis=diagnosis,
        incident_context={
            "incident_id": "inc_llm_002",
            "source": "spark",
            "cluster": "dw-prod",
            "job_name": "dws_user_growth_nightly",
            "application_id": "application_001",
        },
        client=mock_client,
    )

    assert isinstance(result, HealActionPlan)
    assert result.action_type == "tune_and_retry"
    assert result.target_system == "scheduler"
    assert result.parameters["executor_memory_gb"] == 10

    call_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert call_kwargs["response_model"] is HealActionPlan
    assert call_kwargs["messages"][0]["role"] == "system"
    assert call_kwargs["messages"][1]["role"] == "user"
