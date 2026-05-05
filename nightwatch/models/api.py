from datetime import datetime

from pydantic import BaseModel, Field

from nightwatch.models.incident import IncidentType


class IncidentPayload(BaseModel):
    cluster: str = Field(..., min_length=1, description="target cluster, e.g. dw-prod")
    job_name: str = Field(..., min_length=1, description="scheduler or compute job name")
    application_id: str = Field(default="", description="Spark/Flink application id")
    severity: str = Field(..., min_length=1, description="incident severity, e.g. P0/P1")
    message: str = Field(..., min_length=1, description="raw alert message")
    incident_type: IncidentType = Field(
        default=IncidentType.UNKNOWN,
        description="normalized incident type if available",
    )
    scheduler_task_id: str = Field(default="", description="scheduler task identifier")
    host: str = Field(default="", description="source host of alert")
    event_time: datetime | None = Field(default=None, description="event time in ISO format")
    labels: dict[str, str] = Field(default_factory=dict, description="extra alert labels")


class IncidentIngestRequest(BaseModel):
    source: str = Field(..., min_length=1, description="alert source, e.g. spark/flink")
    payload: IncidentPayload = Field(..., description="validated incident payload")
    auto_execute: bool = Field(default=False, description="whether to run in auto mode")


class IncidentResponse(BaseModel):
    incident_id: str = Field(..., description="generated incident id")
    status: str = Field(..., description="state machine final status")
    incident_type: str = Field(..., description="diagnosed incident type")
    severity: str = Field(..., description="incident severity")
    diagnosis_summary: str = Field(default="", description="diagnosis summary from RCA")
    recommended_action: str = Field(default="", description="recommended heal action")
    approval_required: bool = Field(default=True, description="whether approval is required")
    execution_result: dict[str, object] | None = Field(default=None, description="execution record if any")
    state_snapshot: dict[str, object] = Field(
        default_factory=dict,
        description="core AgentState snapshot for UI monitoring",
    )
    error: str | None = Field(default=None, description="error message if state machine failed")


class ActionApprovalRequest(BaseModel):
    incident_id: str = Field(..., min_length=1)
    approver: str = Field(..., min_length=1)
    approved: bool = Field(...)
    comment: str = Field(default="")


class HealthResponse(BaseModel):
    status: str = Field(...)
    connectors_connected: list[str] = Field(default_factory=list)
    vector_store_count: int = Field(default=0, ge=0)
    pending_incidents: int = Field(default=0, ge=0)


class RootCauseDiagnosis(BaseModel):
    incident_type: str = Field(..., description="normalized incident type")
    summary: str = Field(..., description="short diagnosis summary")
    root_cause: str = Field(..., description="concrete root cause")
    confidence: float = Field(..., ge=0.0, le=1.0, description="confidence score")
    evidence: list[str] = Field(default_factory=list, description="evidence chain from logs")
    risk_level: str = Field(..., description="low / medium / high / critical")
    recoverable: bool = Field(..., description="whether automated recovery is possible")
    requires_circuit_break: bool = Field(default=False, description="whether to enter circuit break flow")


class HealActionPlan(BaseModel):
    action_type: str = Field(..., description="tune_and_retry / restart / pause / isolate / escalate")
    target_system: str = Field(..., description="spark / flink / scheduler / catalog")
    target_id: str = Field(..., description="target task or application identifier")
    parameters: dict[str, object] = Field(default_factory=dict, description="action parameters")
    reason: str = Field(..., description="why this action is chosen")
    cooldown_seconds: int = Field(default=0, ge=0, description="cooldown before next action")
    requires_approval: bool = Field(default=True, description="whether human approval is required")


class RuntimeConnectorConfig(BaseModel):
    connectors_demo_fallback: bool = Field(..., description="whether connector calls may use demo fallback")

    scheduler_backend: str = Field(..., description="scheduler backend name, e.g. airflow")
    scheduler_base_url: str = Field(default="", description="scheduler API base URL")
    scheduler_api_token: str = Field(default="", description="masked scheduler API token")
    scheduler_username: str = Field(default="", description="scheduler username")
    scheduler_password: str = Field(default="", description="masked scheduler password")
    scheduler_verify_tls: bool = Field(..., description="whether scheduler TLS cert should be verified")

    spark_history_base_url: str = Field(default="", description="Spark History Server base URL")
    spark_yarn_rm_base_url: str = Field(default="", description="YARN ResourceManager base URL")
    spark_control_base_url: str = Field(default="", description="Spark control API base URL")
    spark_api_token: str = Field(default="", description="masked Spark API token")
    spark_verify_tls: bool = Field(..., description="whether Spark TLS cert should be verified")

    flink_base_url: str = Field(default="", description="Flink REST API base URL")
    flink_api_token: str = Field(default="", description="masked Flink API token")
    flink_verify_tls: bool = Field(..., description="whether Flink TLS cert should be verified")

    qdrant_url: str = Field(default="", description="Qdrant service URL")
    qdrant_api_key: str = Field(default="", description="masked Qdrant API key")

    llm_base_url: str = Field(default="", description="LLM endpoint base URL")
    llm_model: str = Field(default="", description="LLM model name")
    llm_api_key: str = Field(default="", description="masked LLM API key")


class RuntimeConnectorConfigUpdate(BaseModel):
    connectors_demo_fallback: bool | None = Field(default=None)

    scheduler_backend: str | None = Field(default=None)
    scheduler_base_url: str | None = Field(default=None)
    scheduler_api_token: str | None = Field(default=None)
    scheduler_username: str | None = Field(default=None)
    scheduler_password: str | None = Field(default=None)
    scheduler_verify_tls: bool | None = Field(default=None)

    spark_history_base_url: str | None = Field(default=None)
    spark_yarn_rm_base_url: str | None = Field(default=None)
    spark_control_base_url: str | None = Field(default=None)
    spark_api_token: str | None = Field(default=None)
    spark_verify_tls: bool | None = Field(default=None)

    flink_base_url: str | None = Field(default=None)
    flink_api_token: str | None = Field(default=None)
    flink_verify_tls: bool | None = Field(default=None)

    qdrant_url: str | None = Field(default=None)
    qdrant_api_key: str | None = Field(default=None)

    llm_base_url: str | None = Field(default=None)
    llm_model: str | None = Field(default=None)
    llm_api_key: str | None = Field(default=None)


class RuntimeConfigResponse(BaseModel):
    config: RuntimeConnectorConfig = Field(..., description="effective runtime connector configuration")
    persisted: bool = Field(default=False, description="whether changes survive process restart")
    note: str = Field(default="", description="extra note for operators")
