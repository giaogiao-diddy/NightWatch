from typing import Any, Literal

from pydantic import BaseModel, Field

from nightwatch.models.incident import IncidentEvent


class AgentState(BaseModel):
    """NightWatch 的全局状态，在节点间传递。"""

    incident: IncidentEvent = Field(..., description="当前处理的告警事件")
    mode: Literal["assist", "auto"] = Field(default="assist", description="执行模式")
    conversation_id: str = Field(default="", description="人工介入或追踪会话 ID")

    correlated_events: list[IncidentEvent] = Field(default_factory=list, description="聚合后的关联事件")
    context_bundle: dict[str, Any] = Field(default_factory=dict, description="采集到的上下文信息")
    retrieval_hits: list[dict[str, Any]] = Field(default_factory=list, description="历史案例召回结果")

    diagnosis: dict[str, Any] | None = Field(default=None, description="诊断结果")
    diagnosis_errors: list[str] = Field(default_factory=list, description="诊断阶段错误")

    heal_plan: dict[str, Any] | None = Field(default=None, description="修复计划")
    guardrail_result: dict[str, Any] | None = Field(default=None, description="护栏校验结果")
    execution_records: list[dict[str, Any]] = Field(default_factory=list, description="动作执行记录")
    retry_count: int = Field(default=0, ge=0, description="已执行重试次数")
    max_retries: int = Field(default=2, ge=0, description="最大自动重试次数")

    blast_radius: dict[str, Any] | None = Field(default=None, description="影响范围分析结果")
    notification_targets: list[dict[str, Any]] = Field(default_factory=list, description="通知目标")
    notification_payloads: list[dict[str, Any]] = Field(default_factory=list, description="通知内容")

    status: Literal[
        "pending",
        "diagnosed",
        "healed",
        "isolated",
        "escalated",
        "failed",
    ] = Field(default="pending", description="当前状态")
    error_message: str = Field(default="", description="失败或升级时的错误信息")