from typing import Any

from fastmcp import FastMCP

from nightwatch.connectors.collaboration import mock_create_collaboration_ticket
from nightwatch.connectors.flink import mock_pause_cdc_job
from nightwatch.connectors.scheduler import (
    mock_get_task_status,
    mock_retry_task,
    mock_suspend_downstream_dag,
)
from nightwatch.connectors.spark import mock_tune_spark_parameters


def tune_spark_parameters(
    application_id: str,
    executor_memory_gb: int = 8,
    shuffle_partitions: int = 400,
    skew_optimization: str = "",
) -> dict[str, object]:
    """Tune Spark runtime parameters for a target application.

    This tool applies non-destructive Spark tuning knobs to an existing application
    context and returns the resulting parameter set for audit. It is intended for
    controlled remediation flows where the agent needs a standardized actuation
    boundary before scheduling a retry.
    """

    parameters: dict[str, object] = {
        "executor_memory_gb": executor_memory_gb,
        "shuffle_partitions": shuffle_partitions,
    }
    if skew_optimization.strip():
        parameters["skew_optimization"] = skew_optimization.strip()

    return mock_tune_spark_parameters(application_id=application_id, parameters=parameters)


def retry_scheduler_task(task_id: str, parameters: dict[str, Any] | None = None) -> dict[str, object]:
    """Submit a controlled retry request for a scheduler task.

    Use this tool to enqueue one retry attempt with explicit parameters so execution
    is fully traceable. The connector returns a mock submission record that mimics
    a real scheduler acknowledgement.
    """

    return mock_retry_task(task_id=task_id, parameters=dict(parameters or {}))


def get_scheduler_task_status(task_id: str) -> dict[str, object]:
    """Fetch current scheduler task status for post-remediation verification.

    This tool is used by recovery verification logic to decide whether the
    prior remediation action has restored the task into a healthy state.
    """

    return mock_get_task_status(task_id=task_id)


def pause_flink_job(job_id: str, reason: str) -> dict[str, object]:
    """Pause a Flink CDC job to stop further data propagation.

    This tool represents the physical circuit-break operation for high-risk
    incidents such as schema drift or contamination spread. It pauses ingestion
    immediately and returns the mock pause receipt.
    """

    return mock_pause_cdc_job(job_id=job_id, reason=reason)


def suspend_downstream_dag(dag_id: str, reason: str) -> dict[str, object]:
    """Suspend a downstream DAG to contain blast radius.

    Call this tool when upstream pipeline quality is uncertain and downstream
    materialization must be frozen to prevent corruption amplification.
    """

    return mock_suspend_downstream_dag(dag_id=dag_id, reason=reason)


def create_github_issue(owner: str, title: str, body: str, labels: list[str] | None = None) -> dict[str, object]:
    """Create an accountability issue in GitHub for incident coordination.

    This tool generates a structured owner-facing issue with evidence context,
    intended for precise responsibility routing during incident escalation.
    """

    return mock_create_collaboration_ticket(
        system="github",
        owner=owner,
        title=title,
        body=body,
        labels=labels,
    )


def create_jira_issue(owner: str, title: str, body: str, labels: list[str] | None = None) -> dict[str, object]:
    """Create a Jira issue for production incident workflow integration.

    Use this tool when the organization relies on Jira as the primary incident
    command channel. The output includes a mock ticket identifier for linkage.
    """

    return mock_create_collaboration_ticket(
        system="jira",
        owner=owner,
        title=title,
        body=body,
        labels=labels,
    )


TOOL_HANDLERS: dict[str, Any] = {
    "tune_spark_parameters": tune_spark_parameters,
    "retry_scheduler_task": retry_scheduler_task,
    "get_scheduler_task_status": get_scheduler_task_status,
    "pause_flink_job": pause_flink_job,
    "suspend_downstream_dag": suspend_downstream_dag,
    "create_github_issue": create_github_issue,
    "create_jira_issue": create_jira_issue,
}


def register_tools(mcp: FastMCP) -> None:
    """Register NightWatch operational tools on a FastMCP server instance."""

    for tool_name, handler in TOOL_HANDLERS.items():
        mcp.tool(name=tool_name)(handler)
