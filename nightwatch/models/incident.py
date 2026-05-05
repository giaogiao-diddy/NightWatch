from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class IncidentType(str, Enum):
    SPARK_OOM = "spark_oom"
    SPARK_SKEW = "spark_skew"
    DRIVER_CONTENTION = "driver_contention"
    FLINK_BACKPRESSURE = "flink_backpressure"
    FLINK_SCHEMA_DRIFT = "flink_schema_drift"
    PAIMON_WRITE_STALL = "paimon_write_stall"
    UNKNOWN = "unknown"


class LogSnippet(BaseModel):
    source: str = Field(..., description="日志来源，如 yarn 或 flink_taskmanager")
    host: str = Field(default="", description="日志节点主机名")
    offset_start: int = Field(default=0, ge=0, description="日志切片起始偏移")
    offset_end: int = Field(default=0, ge=0, description="日志切片结束偏移")
    content: str = Field(..., description="日志正文")
    score: float = Field(default=0.0, description="日志片段相关性评分")


class MetricSnapshot(BaseModel):
    name: str = Field(..., description="指标名称")
    value: float = Field(..., description="指标数值")
    unit: str = Field(default="", description="指标单位")
    ts: datetime = Field(..., description="指标采集时间")


class IncidentEvent(BaseModel):
    incident_id: str = Field(..., description="唯一事件 ID")
    source: str = Field(..., description="事件来源，如 spark 或 flink")
    incident_type: IncidentType = Field(default=IncidentType.UNKNOWN, description="事件类型")
    severity: str = Field(..., description="事件等级，如 P0、P1、P2")
    cluster: str = Field(..., description="所属集群")
    job_name: str = Field(..., description="任务名称")
    application_id: str = Field(default="", description="Spark 或 Flink 应用 ID")
    scheduler_task_id: str = Field(default="", description="调度器任务 ID")
    event_time: datetime = Field(..., description="事件发生时间")
    error_signature: str = Field(default="", description="归一化错误签名")
    labels: dict[str, str] = Field(default_factory=dict, description="事件标签")
    log_snippets: list[LogSnippet] = Field(default_factory=list, description="关联日志片段")
    metrics: list[MetricSnapshot] = Field(default_factory=list, description="关联指标快照")