import os

from fastmcp import FastMCP

from nightwatch.mcp.registry import Role, register_tools_for_role, resolve_role


def create_mcp_server(role: Role | None = None) -> FastMCP:
    mcp = FastMCP("nightwatch-control-plane")
    effective_role = role or resolve_role(os.getenv("NW_MCP_ROLE", Role.AGENT.value))
    register_tools_for_role(mcp, role=effective_role)
    return mcp


def main() -> None:
    mcp = create_mcp_server()
    transport = os.getenv("NW_MCP_TRANSPORT", "streamable-http")

    if transport == "stdio":
        mcp.run(transport="stdio")
        return

    host = os.getenv("NW_MCP_HOST", "127.0.0.1")
    port = int(os.getenv("NW_MCP_PORT", "9001"))

    mcp.run(
        transport=transport,
        host=host,
        port=port,
        show_banner=True,
    )


if __name__ == "__main__":
    main()
