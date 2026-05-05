from datetime import datetime

import pytest

from nightwatch.agent.graph import run_minimal_loop
from nightwatch.diagnostics.spark_rca import LLMServiceError as DiagnoseLLMServiceError
from nightwatch.mcp.client import get_local_mcp_client
from nightwatch.models.incident import IncidentEvent, IncidentType, LogSnippet
from nightwatch.models.state import AgentState


@pytest.mark.asyncio
async def test_schema_drift_loop_routes_to_circuit_break_and_owner_notification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called_tools: list[str] = []

    def mock_analyze_root_cause_with_llm(*args, **kwargs):  # noqa: ANN002, ANN003
        raise DiagnoseLLMServiceError("llm timeout")

    monkeypatch.setattr(
        "nightwatch.agent.nodes.analyze_root_cause_with_llm",
        mock_analyze_root_cause_with_llm,
    )

    async def mock_call_tool(self, name: str, arguments: dict[str, object] | None = None):
        called_tools.append(name)
        normalized_args = dict(arguments or {})

        if name == "pause_flink_job":
            return {
                "job_id": normalized_args.get("job_id", ""),
                "result": "paused",
                "reason": normalized_args.get("reason", ""),
            }
        if name == "suspend_downstream_dag":
            return {
                "dag_id": normalized_args.get("dag_id", ""),
                "result": "suspended",
                "reason": normalized_args.get("reason", ""),
            }
        if name in {"create_jira_issue", "create_github_issue"}:
            return {
                "system": "jira" if name == "create_jira_issue" else "github",
                "ticket_id": f"{name}-ticket",
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
        incident_id="inc_schema_drift_001",
        source="flink",
        incident_type=IncidentType.FLINK_SCHEMA_DRIFT,
        severity="P0",
        cluster="dw-prod",
        job_name="ods.orders",
        application_id="flink_cdc_orders",
        event_time=datetime(2026, 5, 4, 4, 0, 0),
        error_signature=(
            "Unsupported CDC data type; table=ods.orders, column=total_amount, "
            "old_type=DECIMAL(10,2), new_type=JSON"
        ),
        log_snippets=[
            LogSnippet(
                source="flink_taskmanager",
                host="tm-01",
                offset_start=88,
                offset_end=92,
                content=(
                    "Unsupported CDC data type; table=ods.orders, column=total_amount, "
                    "old_type=DECIMAL(10,2), new_type=JSON"
                ),
            )
        ],
    )

    final_state = await run_minimal_loop(AgentState(incident=incident))

    assert final_state.status in {"isolated", "escalated"}
    assert final_state.diagnosis is not None
    assert final_state.diagnosis["requires_circuit_break"] is True

    assert final_state.blast_radius is not None
    assert final_state.blast_radius["drifted_table"] == "ods.orders"
    assert final_state.blast_radius["upstream_owner"] == "team.ordering_platform"
    assert final_state.blast_radius["affected_starrocks_dashboards"]

    assert final_state.notification_payloads
    accountability_report = final_state.notification_payloads[0]
    assert accountability_report["upstream_owner"] == "team.ordering_platform"
    assert accountability_report["affected_starrocks_dashboards"]
    assert "Unsupported CDC data type" in accountability_report["error_signature"]

    action_types = [record["action_type"] for record in final_state.execution_records]
    assert "circuit_break" in action_types
    assert "notify_owners" in action_types
    assert "pause_flink_job" in called_tools
    assert "suspend_downstream_dag" in called_tools
    assert "create_jira_issue" in called_tools
