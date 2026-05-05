from typing import Any

from nightwatch.agent.policies import CONTROLLED_RETRY_ACTION, RESTART_ACTION, TUNE_AND_RETRY_ACTION
from nightwatch.mcp.client import LocalMCPClient, get_local_mcp_client


async def execute_controlled_retry(
    heal_plan: dict[str, object],
    mcp_client: LocalMCPClient | None = None,
) -> dict[str, object]:
    target_id = str(heal_plan.get("target_id", ""))
    parameters = heal_plan.get("parameters", {})
    action_type = str(heal_plan.get("action_type", ""))

    if not target_id:
        raise ValueError("missing target_id in heal plan")
    if not isinstance(parameters, dict):
        raise ValueError("heal plan parameters must be a dict")
    if action_type not in {CONTROLLED_RETRY_ACTION, TUNE_AND_RETRY_ACTION, RESTART_ACTION}:
        raise ValueError(f"unsupported action_type: {action_type}")

    client = mcp_client or get_local_mcp_client()
    tool_results: dict[str, Any] = {}

    if action_type == TUNE_AND_RETRY_ACTION:
        tune_result = await client.call_tool(
            name="tune_spark_parameters",
            arguments={
                "application_id": target_id,
                "executor_memory_gb": int(parameters.get("executor_memory_gb", 8)),
                "shuffle_partitions": int(parameters.get("shuffle_partitions", 400)),
                "skew_optimization": str(parameters.get("skew_optimization", "")),
            },
        )
        tool_results["tune_spark_parameters"] = tune_result

    retry_result = await client.call_tool(
        name="retry_scheduler_task",
        arguments={
            "task_id": target_id,
            "parameters": parameters,
        },
    )
    tool_results["retry_scheduler_task"] = retry_result

    return {
        "action_type": action_type,
        "target_id": target_id,
        "result": str(retry_result.get("result", "unknown")),
        "parameters": parameters,
        "mcp_tool_results": tool_results,
    }