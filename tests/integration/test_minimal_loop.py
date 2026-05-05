from datetime import datetime

import pytest

from nightwatch.agent.graph import run_minimal_loop
from nightwatch.mcp.client import get_local_mcp_client
from nightwatch.models.api import HealActionPlan, RootCauseDiagnosis
from nightwatch.models.incident import IncidentEvent, IncidentType, LogSnippet
from nightwatch.models.state import AgentState


@pytest.mark.asyncio
async def test_minimal_loop_runs_full_chain_with_manual_gate_for_high_risk_spark_skew(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called_tools: list[str] = []

    def mock_analyze_root_cause_with_llm(*args, **kwargs) -> RootCauseDiagnosis:  # noqa: ANN002, ANN003
        return RootCauseDiagnosis(
            incident_type=IncidentType.SPARK_SKEW.value,
            summary="Spark skew pattern detected",
            root_cause="Shuffle stage contains oversized or skewed partitions",
            confidence=0.95,
            evidence=["Detected skewed partition 42 with size 6.2GB during shuffle stage."],
            risk_level="high",
            recoverable=True,
            requires_circuit_break=False,
        )

    def mock_generate_heal_plan_with_llm(*args, **kwargs) -> HealActionPlan:  # noqa: ANN002, ANN003
        return HealActionPlan(
            action_type="tune_and_retry",
            target_system="scheduler",
            target_id="dws_user_growth_nightly",
            reason="mock_plan_for_spark_skew",
            parameters={
                "executor_memory_gb": 12,
                "shuffle_partitions": 1200,
                "skew_optimization": "salting",
            },
            cooldown_seconds=0,
            requires_approval=True,
        )

    monkeypatch.setattr(
        "nightwatch.agent.nodes.analyze_root_cause_with_llm",
        mock_analyze_root_cause_with_llm,
    )
    monkeypatch.setattr(
        "nightwatch.agent.nodes.generate_heal_plan_with_llm",
        mock_generate_heal_plan_with_llm,
    )

    async def mock_call_tool(self, name: str, arguments: dict[str, object] | None = None):
        called_tools.append(name)
        normalized_args = dict(arguments or {})
        if name == "tune_spark_parameters":
            return {
                "application_id": normalized_args.get("application_id", ""),
                "result": "applied",
                "parameters": {
                    "executor_memory_gb": normalized_args.get("executor_memory_gb", 8),
                    "shuffle_partitions": normalized_args.get("shuffle_partitions", 400),
                    "skew_optimization": normalized_args.get("skew_optimization", ""),
                },
            }
        if name == "retry_scheduler_task":
            return {
                "task_id": normalized_args.get("task_id", ""),
                "result": "submitted",
                "parameters": normalized_args.get("parameters", {}),
            }
        if name == "get_scheduler_task_status":
            return {
                "task_id": normalized_args.get("task_id", ""),
                "status": "SUCCESS",
                "result": "queried",
            }
        raise AssertionError(f"unexpected tool call: {name}")

    monkeypatch.setattr(
        "nightwatch.mcp.client.LocalMCPClient.call_tool",
        mock_call_tool,
    )
    get_local_mcp_client.cache_clear()

    incident = IncidentEvent(
        incident_id="inc_minimal_001",
        source="spark",
        incident_type=IncidentType.UNKNOWN,
        severity="P0",
        cluster="dw-prod",
        job_name="dws_user_growth_nightly",
        application_id="application_1714858800_1024",
        event_time=datetime(2026, 5, 4, 3, 0, 0),
        error_signature="Detected skewed partition during shuffle stage",
        labels={"approval": "approved"},
        log_snippets=[
            LogSnippet(
                source="yarn",
                host="worker-01",
                offset_start=0,
                offset_end=1,
                content="Detected skewed partition 42 with size 6.2GB during shuffle stage.",
            )
        ],
    )
    initial_state = AgentState(incident=incident)

    final_state = await run_minimal_loop(initial_state)

    assert final_state.status == "healed"
    assert final_state.retry_count == 1
    assert final_state.diagnosis is not None
    assert final_state.diagnosis["incident_type"] == IncidentType.SPARK_SKEW.value
    assert final_state.heal_plan is not None
    assert final_state.heal_plan["action_type"] == "tune_and_retry"
    assert final_state.guardrail_result is not None
    assert final_state.guardrail_result["decision"] == "require_approval"
    assert final_state.guardrail_result["approval_status"] == "approved"
    assert len(final_state.execution_records) == 2
    assert final_state.execution_records[0]["action_type"] == "tune_and_retry"
    assert final_state.execution_records[0]["result"] == "submitted"
    assert final_state.execution_records[1]["action_type"] == "verify_recovery"
    assert final_state.execution_records[1]["recovered"] is True
    assert called_tools == [
        "tune_spark_parameters",
        "retry_scheduler_task",
        "get_scheduler_task_status",
    ]