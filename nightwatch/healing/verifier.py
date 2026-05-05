from typing import Any

from nightwatch.mcp.client import LocalMCPClient, get_local_mcp_client


RECOVERED_STATES = {"RUNNING", "SUCCESS"}


async def verify_recovery(
    target_id: str,
    mcp_client: LocalMCPClient | None = None,
) -> dict[str, Any]:
    if not target_id.strip():
        raise ValueError("target_id cannot be empty for recovery verification")

    client = mcp_client or get_local_mcp_client()
    status_payload = await client.call_tool(
        name="get_scheduler_task_status",
        arguments={"task_id": target_id},
    )

    raw_status = str(status_payload.get("status", "UNKNOWN")).upper()
    recovered = raw_status in RECOVERED_STATES

    return {
        "action_type": "verify_recovery",
        "target_id": target_id,
        "status": raw_status,
        "recovered": recovered,
        "result": "verified" if recovered else "degraded",
        "mcp_tool_results": {
            "get_scheduler_task_status": status_payload,
        },
    }