from typing import Any

from nightwatch.mcp.client import LocalMCPClient, get_local_mcp_client


async def notify_accountability_owners(
    owners: list[str],
    title: str,
    body: str,
    labels: list[str] | None = None,
    mcp_client: LocalMCPClient | None = None,
) -> list[dict[str, Any]]:
    """Dispatch owner accountability tickets through MCP tools.

    The first owner is sent to Jira for primary incident command, while
    subsequent owners are mirrored to GitHub for engineering traceability.
    """

    normalized_owners = [owner.strip() for owner in owners if owner.strip()]
    if not normalized_owners:
        return []

    client = mcp_client or get_local_mcp_client()
    tickets: list[dict[str, Any]] = []

    for index, owner in enumerate(normalized_owners):
        tool_name = "create_jira_issue" if index == 0 else "create_github_issue"
        ticket = await client.call_tool(
            name=tool_name,
            arguments={
                "owner": owner,
                "title": title,
                "body": body,
                "labels": labels or [],
            },
        )
        tickets.append({"tool": tool_name, **ticket})

    return tickets
