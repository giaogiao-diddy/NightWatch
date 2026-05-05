from typing import Any

from nightwatch.mcp.client import LocalMCPClient, get_local_mcp_client
from nightwatch.models.incident import IncidentEvent


async def execute_circuit_break(
    incident: IncidentEvent,
    blast_radius: dict[str, Any],
    mcp_client: LocalMCPClient | None = None,
) -> dict[str, Any]:
    reason = (
        "schema drift detected; pause CDC ingest and suspend downstream DAGs "
        "to prevent data contamination"
    )

    cdc_jobs = list(blast_radius.get("cdc_jobs", []))
    target_cdc_job = cdc_jobs[0] if cdc_jobs else (incident.application_id or incident.job_name)

    client = mcp_client or get_local_mcp_client()

    pause_result = await client.call_tool(
        name="pause_flink_job",
        arguments={
            "job_id": target_cdc_job,
            "reason": reason,
        },
    )

    suspended_dags: list[dict[str, object]] = []
    for dag_id in blast_radius.get("dependent_dags", []):
        suspended_dags.append(
            await client.call_tool(
                name="suspend_downstream_dag",
                arguments={
                    "dag_id": dag_id,
                    "reason": reason,
                },
            )
        )

    all_suspended = all(item.get("result") == "suspended" for item in suspended_dags) if suspended_dags else True
    isolated = pause_result.get("result") == "paused" and all_suspended

    return {
        "action_type": "circuit_break",
        "target_id": target_cdc_job,
        "reason": reason,
        "paused_cdc": pause_result,
        "suspended_dags": suspended_dags,
        "result": "isolated" if isolated else "partial",
    }
