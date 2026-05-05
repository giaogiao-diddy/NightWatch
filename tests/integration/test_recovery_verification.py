from datetime import datetime

import pytest

from nightwatch.agent.graph import run_minimal_loop
from nightwatch.mcp.client import get_local_mcp_client
from nightwatch.models.api import HealActionPlan, RootCauseDiagnosis
from nightwatch.models.incident import IncidentEvent, IncidentType, LogSnippet
from nightwatch.models.state import AgentState


@pytest.mark.asyncio
async def test_recovery_verification_failure_escalates_and_notifies_owners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called_tools: list[str] = []

    def mock_analyze_root_cause_with_llm(*args, **kwargs) -> RootCauseDiagnosis:  # noqa: ANN002, ANN003
        return RootCauseDiagnosis(
            incident_type=IncidentType.SPARK_OOM.value,
            summary="Spark executor memory exhausted",
            root_cause="Executor heap cannot hold current shuffle payload",
            confidence=0.91,
            evidence=["ExecutorLostFailure: Container killed by YARN for exceeding memory limits"],
            risk_level="medium",
            recoverable=True,
            requires_circuit_break=False,
        )

    def mock_generate_heal_plan_with_llm(*args, **kwargs) -> HealActionPlan:  # noqa: ANN002, ANN003
        return HealActionPlan(
            action_type="tune_and_retry",
            target_system="scheduler",
            target_id="dws_user_growth_nightly",
            reason="retry after memory tuning",
            parameters={
                "executor_memory_gb": 10,
                "shuffle_partitions": 800,
            },
            cooldown_seconds=0,
            requires_approval=False,
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
                "status": "STUCK",
                "result": "queried",
            }

        if name == "create_jira_issue":
            return {
                "system": "jira",
                "ticket_id": "JIRA-escalation-001",
                "owner": normalized_args.get("owner", ""),
                "result": "created",
            }

        if name == "create_github_issue":
            return {
                "system": "github",
                "ticket_id": "GH-escalation-001",
                "owner": normalized_args.get("owner", ""),
                "result": "created",
            }

        raise AssertionError(f"unexpected tool call: {name}")

    monkeypatch.setattr(
        "nightwatch.mcp.client.LocalMCPClient.call_tool",
        mock_call_tool,
    )
    get_local_mcp_client.cache_clear()

    incident = IncidentEvent(
        incident_id="inc_recovery_verify_001",
        source="spark",
        incident_type=IncidentType.SPARK_OOM,
        severity="P0",
        cluster="dw-prod",
        job_name="dws_user_growth_nightly",
        application_id="application_1714858800_1024",
        event_time=datetime(2026, 5, 4, 3, 10, 0),
        error_signature="ExecutorLostFailure: Container killed by YARN for exceeding memory limits",
        labels={"approval": "approved"},
        log_snippets=[
            LogSnippet(
                source="yarn",
                host="worker-01",
                offset_start=0,
                offset_end=1,
                content="ExecutorLostFailure: Container killed by YARN for exceeding memory limits",
                score=0.94,
            )
        ],
    )

    final_state = await run_minimal_loop(AgentState(incident=incident))

    assert final_state.status == "escalated"
    assert "recovery verification failed" in final_state.error_message
    assert final_state.notification_payloads
    assert any(record.get("action_type") == "verify_recovery" for record in final_state.execution_records)
    assert any(record.get("action_type") == "notify_owners" for record in final_state.execution_records)
    assert "get_scheduler_task_status" in called_tools
    assert "create_jira_issue" in called_tools