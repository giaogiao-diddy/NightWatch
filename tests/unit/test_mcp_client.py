import pytest

from nightwatch.mcp.client import LocalMCPClient, MCPToolCallError
from nightwatch.mcp.server import create_mcp_server


@pytest.mark.asyncio
async def test_local_mcp_client_calls_registered_tool() -> None:
    client = LocalMCPClient(app=create_mcp_server())

    result = await client.call_tool(
        name="retry_scheduler_task",
        arguments={
            "task_id": "dag_unit_001",
            "parameters": {"executor_memory_gb": 8},
        },
    )

    assert result["task_id"] == "dag_unit_001"
    assert result["result"] == "submitted"


@pytest.mark.asyncio
async def test_local_mcp_client_raises_when_tool_missing() -> None:
    client = LocalMCPClient(app=create_mcp_server())

    with pytest.raises(MCPToolCallError):
        await client.call_tool(name="unknown_tool", arguments={})
