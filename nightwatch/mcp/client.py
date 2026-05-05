import json
from functools import lru_cache
from typing import Any

from fastmcp import FastMCP

from nightwatch.mcp.server import create_mcp_server


class MCPToolCallError(RuntimeError):
    """Raised when local MCP tool invocation fails."""


class LocalMCPClient:
    """In-process MCP client adapter with a JSON-RPC style call interface.

    This adapter binds to a local FastMCP app instance and exposes a generic
    call_tool(name, arguments) method so agent nodes execute tools through a
    standard MCP boundary rather than importing connector modules directly.
    """

    def __init__(self, app: FastMCP | None = None) -> None:
        self._app = app or create_mcp_server()

    async def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized_arguments = dict(arguments or {})

        try:
            tool = await self._app.get_tool(name)
        except Exception as exc:  # noqa: BLE001
            raise MCPToolCallError(f"failed to resolve MCP tool: {name}") from exc

        try:
            result = await tool.run(normalized_arguments)
        except Exception as exc:  # noqa: BLE001
            raise MCPToolCallError(f"MCP tool '{name}' invocation failed") from exc

        payload = result.structured_content
        if isinstance(payload, dict):
            return payload

        if isinstance(payload, list):
            return {"items": payload}

        for content in getattr(result, "content", []):
            text = getattr(content, "text", "")
            if not text:
                continue
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed

        raise MCPToolCallError(f"MCP tool '{name}' returned unsupported payload")


@lru_cache
def get_local_mcp_client() -> LocalMCPClient:
    return LocalMCPClient()
