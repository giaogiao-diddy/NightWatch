from enum import Enum

from fastmcp import FastMCP
from pydantic import BaseModel, Field

from nightwatch.mcp.tools import TOOL_HANDLERS


class Role(str, Enum):
    AGENT = "agent"
    ONCALL = "oncall"
    ADMIN = "admin"


class ToolPermission(BaseModel):
    name: str
    allowed_roles: set[Role] = Field(default_factory=set)
    description: str = Field(default="")


TOOL_PERMISSIONS: dict[str, ToolPermission] = {
    "tune_spark_parameters": ToolPermission(
        name="tune_spark_parameters",
        allowed_roles={Role.AGENT, Role.ONCALL, Role.ADMIN},
        description="Tune Spark runtime parameters within guardrail boundaries.",
    ),
    "retry_scheduler_task": ToolPermission(
        name="retry_scheduler_task",
        allowed_roles={Role.AGENT, Role.ONCALL, Role.ADMIN},
        description="Submit one controlled scheduler retry for incident remediation.",
    ),
    "get_scheduler_task_status": ToolPermission(
        name="get_scheduler_task_status",
        allowed_roles={Role.AGENT, Role.ONCALL, Role.ADMIN},
        description="Read scheduler task status for post-remediation verification.",
    ),
    "pause_flink_job": ToolPermission(
        name="pause_flink_job",
        allowed_roles={Role.AGENT, Role.ONCALL, Role.ADMIN},
        description="Pause CDC ingest to contain high-risk data incidents.",
    ),
    "suspend_downstream_dag": ToolPermission(
        name="suspend_downstream_dag",
        allowed_roles={Role.AGENT, Role.ONCALL, Role.ADMIN},
        description="Suspend dependent DAGs during circuit-break operations.",
    ),
    "create_github_issue": ToolPermission(
        name="create_github_issue",
        allowed_roles={Role.AGENT, Role.ONCALL, Role.ADMIN},
        description="Create owner accountability issue in GitHub.",
    ),
    "create_jira_issue": ToolPermission(
        name="create_jira_issue",
        allowed_roles={Role.AGENT, Role.ONCALL, Role.ADMIN},
        description="Create owner accountability issue in Jira.",
    ),
}


def resolve_role(raw_role: str) -> Role:
    normalized = raw_role.strip().lower()
    try:
        return Role(normalized)
    except ValueError:
        return Role.AGENT


def is_tool_allowed(role: Role, tool_name: str) -> bool:
    permission = TOOL_PERMISSIONS.get(tool_name)
    if permission is None:
        return False
    return role in permission.allowed_roles


def register_tools_for_role(mcp: FastMCP, role: Role) -> None:
    for tool_name, handler in TOOL_HANDLERS.items():
        if not is_tool_allowed(role=role, tool_name=tool_name):
            continue
        mcp.tool(name=tool_name)(handler)