# NightWatch 技术设计文档

## 1. 技术栈总览

| 层级            | 组件                          | 版本要求 | PyPI 包名 / 系统组件                         |
| --------------- | ----------------------------- | -------- | -------------------------------------------- |
| Agent 编排      | LangGraph                     | ≥0.2     | `langgraph`, `langchain-core`                |
| LLM 调用        | LangChain OpenAI 兼容         | —        | `langchain-openai`                           |
| 结构化输出      | Instructor + Pydantic v2      | ≥1.0     | `instructor`, `pydantic>=2.0`                |
| MCP 协议层      | FastMCP                       | ≥2.0     | `fastmcp`                                    |
| 向量库          | Qdrant                        | ≥1.9     | `qdrant-client`                              |
| 关键词检索      | BM25                          | —        | `rank-bm25`                                  |
| 嵌入模型        | BGE-large-zh-v1.5             | —        | `sentence-transformers`                      |
| Web 框架        | FastAPI + Uvicorn             | ≥0.110   | `fastapi`, `uvicorn`                         |
| 前端            | Gradio                        | ≥4.0     | `gradio`                                     |
| 可观测性        | Langfuse + Prometheus         | ≥2.0     | `langfuse`, `prometheus-client`              |
| 重试控制        | Tenacity                      | ≥8.0     | `tenacity`                                   |
| 配置管理        | Pydantic Settings             | ≥2.0     | `pydantic-settings`                          |
| 日志处理        | Structlog + Orjson            | ≥24.0    | `structlog`, `orjson`                        |
| Spark 接入      | YARN RM / Spark History API   | 集群能力 | 外部系统接口                                 |
| Flink 接入      | Flink REST API + Flink CDC    | 集群能力 | 外部系统接口                                 |
| 湖仓存储        | Apache Paimon                 | 集群能力 | 外部系统组件                                 |
| 实时分析引擎    | StarRocks                     | 集群能力 | 外部系统组件                                 |
| 元数据与血缘    | Hive Metastore / 统一数据目录 | 集群能力 | 外部系统接口                                 |
| 调度系统        | Airflow / DolphinScheduler    | 集群能力 | 外部系统接口                                 |
| 协同系统        | GitHub / Jira / Slack / 飞书  | SaaS API | `PyGithub`, `jira`, Webhook`                 |

---

## 2. 项目目录结构

nightwatch/
├── main.py                         # FastAPI 应用入口
├── worker.py                       # 后台事件消费与自动修复 worker
├── config.py                       # 全局配置（Pydantic Settings）
├── models/                         # Pydantic 数据模型
│   ├── incident.py                 # 告警事件、日志切片、指标快照
│   ├── action.py                   # 修复动作、熔断动作、审计记录
│   ├── lineage.py                  # 血缘节点、影响范围、责任人模型
│   ├── api.py                      # 请求/响应模型
│   └── state.py                    # LangGraph 状态定义
├── agent/                          # Agent 核心
│   ├── graph.py                    # LangGraph 状态机定义
│   ├── nodes.py                    # 各节点实现（采集/诊断/修复/通知）
│   ├── policies.py                 # 路由策略、阈值策略、动作白名单
│   └── prompts.py                  # MVP 阶段 Prompt 常量
├── ingest/                         # 事件接入与归一化
│   ├── normalizer.py               # 多来源告警格式归一化
│   ├── correlator.py               # 告警聚合与 incident 合并
│   └── signatures.py               # 错误签名抽取与去重
├── retrieval/                      # 日志检索与知识库
│   ├── slicer.py                   # 日志窗口切片与摘要
│   ├── embedder.py                 # 嵌入模型封装
│   ├── indexer.py                  # 历史 incident / runbook 入库
│   └── retriever.py                # BM25 + 向量混合检索
├── diagnostics/                    # 故障诊断器
│   ├── spark_rca.py                # Spark OOM / skew / driver contention 诊断
│   ├── flink_rca.py                # Flink checkpoint / backpressure / CDC 异常诊断
│   ├── schema_drift.py             # DDL 漂移与兼容性识别
│   └── scorer.py                   # 诊断置信度与证据评分
├── healing/                        # 自动恢复与安全执行
│   ├── planner.py                  # 生成 Try-Heal-Retry 修复计划
│   ├── guardrails.py               # 动作白名单、参数上下界、审批门禁
│   ├── executor.py                 # 执行动作（调参、重试、暂停、隔离）
│   └── verifier.py                 # 恢复后健康校验与结果回写
├── lineage/                        # 血缘与责任路由
│   ├── resolver.py                 # 表级/列级血缘解析
│   ├── blast_radius.py             # 爆炸半径分析
│   └── ownership.py                # owner、值班组、责任团队映射
├── connectors/                     # 外部系统适配器
│   ├── base.py                     # Connector 抽象基类
│   ├── yarn.py                     # YARN / Spark History API 适配器
│   ├── flink.py                    # Flink REST API 适配器
│   ├── scheduler.py                # Airflow / DolphinScheduler 适配器
│   ├── catalog.py                  # HMS / 统一目录适配器
│   ├── paimon.py                   # Paimon 元数据与写入状态适配器
│   ├── starrocks.py                # StarRocks 健康检查适配器
│   ├── github.py                   # GitHub Issue / PR 查询与创建
│   └── jira.py                     # Jira issue 创建与状态同步
├── mcp/                            # FastMCP 服务
│   ├── server.py                   # MCP Server 入口
│   ├── tools.py                    # 对外暴露的工具定义
│   └── registry.py                 # 工具注册、权限与路由
├── api/                            # FastAPI 路由
│   ├── incidents.py                # incident 接口
│   ├── actions.py                  # 审批、执行、重试接口
│   ├── knowledge.py                # 知识库同步接口
│   ├── health.py                   # 健康检查接口
│   └── deps.py                     # 依赖注入
├── ui/                             # Gradio 控制台
│   └── app.py                      # 值班摘要面板与事件详情界面
└── tests/
    ├── conftest.py                 # 公共 fixture（mock connector、mock LLM、样例日志）
    ├── unit/
    │   ├── test_slicer.py          # 日志切片单元测试
    │   ├── test_correlator.py      # 告警聚合单元测试
    │   ├── test_spark_rca.py       # Spark 诊断单元测试
    │   ├── test_flink_rca.py       # Flink 诊断单元测试
    │   ├── test_guardrails.py      # 动作护栏单元测试
    │   ├── test_blast_radius.py    # 血缘影响分析单元测试
    │   └── test_state.py           # 状态模型单元测试
    ├── integration/
    │   ├── test_agent_graph.py     # LangGraph 端到端测试
    │   ├── test_mcp_tools.py       # MCP 工具链路测试
    │   └── test_api.py             # FastAPI 接口测试
    └── fixtures/
        ├── spark_oom_logs.txt      # Spark OOM 样例日志
        ├── flink_cdc_drift.json    # Flink CDC Schema Drift 样例事件
        ├── lineage_graph.json      # 血缘图样例
        └── incident_cases.json     # 历史故障案例样本

---

## 3. 核心数据结构

### 3.1 事件模型（`models/incident.py`）

```python
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
    source: str                          # yarn / spark_history / flink_taskmanager
    host: str = ""
    offset_start: int = 0
    offset_end: int = 0
    content: str
    score: float = 0.0                  # 对当前 incident 的相关性打分


class MetricSnapshot(BaseModel):
    name: str                           # executor_lost_count / gc_time_ratio / checkpoint_duration
    value: float
    unit: str = ""
    ts: datetime


class IncidentEvent(BaseModel):
    incident_id: str
    source: str                         # spark / flink / scheduler / catalog
    incident_type: IncidentType = IncidentType.UNKNOWN
    severity: str                       # P0 / P1 / P2
    cluster: str
    job_name: str
    application_id: str = ""
    scheduler_task_id: str = ""
    event_time: datetime
    error_signature: str = ""
    labels: dict[str, str] = Field(default_factory=dict)
    log_snippets: list[LogSnippet] = Field(default_factory=list)
    metrics: list[MetricSnapshot] = Field(default_factory=list)
```

### 3.2 LangGraph 状态（`models/state.py`）

```python
from typing import Literal
from pydantic import BaseModel, Field


class AgentState(BaseModel):
    """NightWatch 的全局状态，在节点间传递。"""

    # 输入
    incident: IncidentEvent
    mode: Literal["assist", "auto"] = "assist"     # MVP 默认为 assist，后续可切 auto
    conversation_id: str = ""                        # 用于事件追踪与人工介入会话

    # 上下文采集
    correlated_events: list[IncidentEvent] = Field(default_factory=list)
    context_bundle: dict = Field(default_factory=dict)     # 调度器状态、Spark stage、Flink checkpoint、Catalog 变更
    retrieval_hits: list[dict] = Field(default_factory=list)

    # 诊断
    diagnosis: dict | None = None                         # 根因、证据、置信度、风险等级
    diagnosis_errors: list[str] = Field(default_factory=list)

    # 执行计划
    heal_plan: dict | None = None                         # 调优参数、重试动作、熔断动作
    guardrail_result: dict | None = None                  # allow / require_approval / block
    execution_records: list[dict] = Field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 2

    # 血缘与通知
    blast_radius: dict | None = None                      # 受影响表、看板、任务、业务域
    notification_targets: list[dict] = Field(default_factory=list)
    notification_payloads: list[dict] = Field(default_factory=list)

    # 结果
    status: Literal[
        "pending",
        "diagnosed",
        "healed",
        "isolated",
        "escalated",
        "failed",
    ] = "pending"
    error_message: str = ""
```

> **Checkpointer 预留说明**：MVP 阶段 `AgentState` 仅在 worker 进程内存中生存。当后续需要支持跨班次追踪、人工确认恢复、二次升级时，在 `graph.compile()` 时传入 `checkpointer` 参数即可启用状态持久化：
>
> ```python
> from langgraph.checkpoint.sqlite import SqliteSaver
> memory = SqliteSaver.from_conn_string("nightwatch_checkpoints.db")
> graph = workflow.compile(checkpointer=memory)
> ```
>
> 通过 `incident_id` 与 `conversation_id` 关联一次故障处理全流程，无需改动节点代码。

### 3.3 请求/响应模型（`models/api.py`）

```python
from pydantic import BaseModel, Field


class IncidentIngestRequest(BaseModel):
    source: str
    payload: dict
    auto_execute: bool = False            # 是否允许进入自动执行模式


class IncidentResponse(BaseModel):
    incident_id: str
    status: str
    incident_type: str
    severity: str
    diagnosis_summary: str = ""
    recommended_action: str = ""
    approval_required: bool = True
    execution_result: dict | None = None
    error: str | None = None


class ActionApprovalRequest(BaseModel):
    incident_id: str
    approver: str
    approved: bool
    comment: str = ""


class HealthResponse(BaseModel):
    status: str
    connectors_connected: list[str] = Field(default_factory=list)
    vector_store_count: int = 0
    pending_incidents: int = 0
```

### 3.4 LLM 结构化输出模型（供 Instructor 使用）

```python
from pydantic import BaseModel, Field


class RootCauseDiagnosis(BaseModel):
    incident_type: str
    summary: str
    root_cause: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[str] = Field(default_factory=list)
    risk_level: str                       # low / medium / high / critical
    recoverable: bool
    requires_circuit_break: bool = False


class HealActionPlan(BaseModel):
    action_type: str                      # tune_and_retry / restart / pause / isolate / escalate
    target_system: str                    # spark / flink / scheduler / catalog
    target_id: str
    parameters: dict = Field(default_factory=dict)
    reason: str
    cooldown_seconds: int = 0
    requires_approval: bool = True


class CircuitBreakDecision(BaseModel):
    enabled: bool
    target_tasks: list[str] = Field(default_factory=list)
    affected_assets: list[str] = Field(default_factory=list)
    blast_radius_summary: str = ""
    downstream_block_required: bool = False


class NotificationDraft(BaseModel):
    channel: str                          # github / jira / slack / feishu
    target: str
    title: str
    body: str
    mentions: list[str] = Field(default_factory=list)
```

---

## 4. LangGraph 状态机设计

### 4.1 节点与边

```text
                    ┌────────────────┐
                    │ START          │
                    └──────┬─────────┘
                           ▼
                    ┌────────────────┐
                    │ ingest_event   │  告警归一化
                    └──────┬─────────┘
                           ▼
                    ┌────────────────┐
                    │ correlate_alert│  聚合重复告警
                    └──────┬─────────┘
                           ▼
                    ┌────────────────┐
                    │ collect_context│  日志/指标/调度信息采集
                    └──────┬─────────┘
                           ▼
                    ┌────────────────┐
                    │ retrieve_cases │  BM25 + 向量混合检索
                    └──────┬─────────┘
                           ▼
                    ┌────────────────┐
                    │ diagnose_rca   │  根因判断
                    └──────┬─────────┘
                           │
                  schema drift / high risk?
                 ┌────yes────┴────no────────┐
                 ▼                           ▼
        ┌────────────────┐          ┌────────────────┐
        │ analyze_blast  │          │ plan_heal      │  生成修复动作
        │ radius         │          └──────┬─────────┘
        └──────┬─────────┘                 ▼
               ▼                    ┌────────────────┐
        ┌────────────────┐          │ guardrail_check│  白名单/审批/阈值
        │ circuit_break  │          └──────┬─────────┘
        └──────┬─────────┘                 │
               ▼                    allow? │
        ┌────────────────┐         ┌──yes──┴──no─────┐
        │ notify_owners  │         ▼                 ▼
        └──────┬─────────┘  ┌────────────────┐  ┌────────────────┐
               ▼            │ execute_heal   │  │ escalate_human │
        ┌────────────────┐  └──────┬─────────┘  └──────┬─────────┘
        │ respond        │         ▼                   ▼
        └────────────────┘  ┌────────────────┐  ┌────────────────┐
                            │ verify_recovery│  │ notify_owners  │
                            └──────┬─────────┘  └──────┬─────────┘
                                   │                  ▼
                              success?           ┌────────────────┐
                           ┌──yes──┴──no────┐    │ respond        │
                           ▼                ▼    └────────────────┘
                    ┌──────────────┐  ┌────────────────┐
                    │ notify_owners│  │ escalate_human │
                    └──────┬───────┘  └──────┬─────────┘
                           ▼                ▼
                    ┌────────────────┐  ┌────────────────┐
                    │ respond        │  │ respond        │
                    └────────────────┘  └────────────────┘
```

### 4.2 节点职责

节点职责如下：

- `ingest_event`：输入原始 webhook / scheduler payload；输出 `incident`；负责归一化不同来源事件，生成标准 incident 对象。
- `correlate_alert`：输入 `incident`；输出 `correlated_events`；负责按 application id、任务名、时间窗口合并同源噪声告警。
- `collect_context`：输入 `incident`、`correlated_events`；输出 `context_bundle`；负责拉取 YARN 日志、Spark stage 指标、Flink checkpoint 与 Catalog 变更。
- `retrieve_cases`：输入 `incident`、`context_bundle`；输出 `retrieval_hits`；负责检索历史案例、runbook、专家修复记录。
- `diagnose_rca`：输入 `context_bundle`、`retrieval_hits`；输出 `diagnosis`；负责输出根因、证据、置信度、是否可恢复、是否需熔断。
- `analyze_blast_radius`：输入 `diagnosis`、`incident`；输出 `blast_radius`、`notification_targets`；负责解析血缘与 owner，计算受影响报表、任务、业务域。
- `plan_heal`：输入 `diagnosis`、`context_bundle`；输出 `heal_plan`；负责生成调优参数、重试动作、cooldown 配置。
- `guardrail_check`：输入 `heal_plan`、`diagnosis`；输出 `guardrail_result`；负责执行动作白名单、参数上界、审批策略和高风险阻断。
- `execute_heal`：输入 `heal_plan`、`guardrail_result`；输出 `execution_records`、`retry_count`；负责通过 FastMCP 执行调参、重启、暂停、隔离动作。
- `verify_recovery`：输入 `execution_records`、`incident`；输出 `status`、`error_message`；负责校验任务恢复、下游表是否产出、StarRocks 查询是否恢复。
- `circuit_break`：输入 `blast_radius`、`diagnosis`；输出 `execution_records`、`status`；负责暂停 Flink CDC、阻断下游 DAG、隔离脏数据批次。
- `notify_owners`：输入 `diagnosis`、`blast_radius`；输出 `notification_payloads`；负责生成 GitHub / Jira / IM 消息并发送。
- `escalate_human`：输入任何失败分支；输出 `status`、`error_message`；负责提升为人工介入并携带完整证据链。
- `respond`：输入全部状态；输出最终结果；负责组装 API 响应、审计记录与控制台展示对象。

### 4.3 日志切片与混合检索策略

夜间故障的主要问题不是“有没有日志”，而是“日志太多且无序”。NightWatch 采用三层日志压缩与检索漏斗，先减噪，再推理：

```text
原始日志流（YARN / Spark Executor / Flink TM）
    │
    ▼
[L1] 时间窗口切片：围绕失败时间点向前后各取 120 秒，并保留 ERROR / WARN 峰值窗口
    │
    ▼
[L2] 错误签名提取：抽取 OOM / Lost executor / checkpoint timeout / unsupported DDL 等关键签名
    │
    ▼
[L3] 混合检索：BM25 命中精确报错；向量检索召回语义相似案例；最后做证据重排
```

```python
from rank_bm25 import BM25Okapi


def extract_relevant_log_slices(raw_lines: list[str], failure_ts: int) -> list[LogSnippet]:
    """围绕故障时间点切片，避免把整份 YARN 日志塞进 LLM。"""
    candidates: list[LogSnippet] = []
    for block in sliding_window(raw_lines, seconds_before=120, seconds_after=120):
        if contains_error_signature(block) or contains_resource_spike(block):
            candidates.append(
                LogSnippet(
                    source=block.source,
                    host=block.host,
                    offset_start=block.start,
                    offset_end=block.end,
                    content=block.text,
                    score=block.score,
                )
            )
    return top_k_by_score(candidates, k=12)


def hybrid_retrieve(query: str, cases: list[str], qdrant_client, embedding) -> list[dict]:
    bm25 = BM25Okapi([case.split() for case in cases])
    lexical_scores = bm25.get_scores(query.split())
    vector_hits = qdrant_client.search(
        collection_name="incident_memory",
        query_vector=embedding,
        limit=10,
    )
    return rerank_hits(lexical_scores, vector_hits)
```

关键点：

- 日志切片只保留“故障前后关键窗口”，避免上下文窗口被无效 INFO 日志吃满。
- 错误签名与资源峰值同时参与筛选，解决单看 ERROR 无法识别 Driver 争抢或 GC 抖动的问题。
- Qdrant 存储历史 incident、runbook、修复动作与最终结果，支持“同类故障优先复用”。

### 4.4 Try-Heal-Retry 动作护栏机制

NightWatch 的核心风险不在诊断，而在错误执行。所有自动化动作必须经过静态白名单 + 动态阈值双重校验。

```python
from pydantic import BaseModel, Field, ValidationError


class SparkTuneAction(BaseModel):
    executor_memory_gb: int = Field(ge=2, le=32)
    executor_cores: int = Field(ge=1, le=8)
    shuffle_partitions: int = Field(ge=50, le=4000)
    enable_aqe: bool = True


ALLOWED_ACTIONS = {
    "spark": {"tune_and_retry", "restart"},
    "flink": {"restart", "pause", "isolate"},
    "scheduler": {"retry_task", "pause_dag"},
}


def validate_heal_plan(plan: HealActionPlan, diagnosis: RootCauseDiagnosis) -> dict:
    if plan.action_type not in ALLOWED_ACTIONS.get(plan.target_system, set()):
        return {"decision": "block", "reason": "action not allowed"}

    if diagnosis.risk_level in {"high", "critical"} and plan.action_type == "tune_and_retry":
        return {"decision": "require_approval", "reason": "high risk incident"}

    if plan.target_system == "spark" and plan.parameters:
        SparkTuneAction(**plan.parameters)

    return {"decision": "allow", "reason": "passed guardrails"}
```

护栏原则：

- Schema Drift、数据污染、未知 DDL 兼容性问题默认不可自动重试，优先熔断或人工确认。
- Spark 资源调优只能在安全边界内做有限增量，禁止一次性把资源翻倍到不可控范围。
- 同一 incident 最多自动尝试 2 次，避免重试风暴拖垮集群。

### 4.5 Graph 定义伪代码

```python
from langgraph.graph import StateGraph, END

workflow = StateGraph(AgentState)

workflow.add_node("ingest_event", ingest_event_node)
workflow.add_node("correlate_alert", correlate_alert_node)
workflow.add_node("collect_context", collect_context_node)
workflow.add_node("retrieve_cases", retrieve_cases_node)
workflow.add_node("diagnose_rca", diagnose_rca_node)
workflow.add_node("analyze_blast_radius", analyze_blast_radius_node)
workflow.add_node("plan_heal", plan_heal_node)
workflow.add_node("guardrail_check", guardrail_check_node)
workflow.add_node("execute_heal", execute_heal_node)
workflow.add_node("verify_recovery", verify_recovery_node)
workflow.add_node("circuit_break", circuit_break_node)
workflow.add_node("notify_owners", notify_owners_node)
workflow.add_node("escalate_human", escalate_human_node)
workflow.add_node("respond", respond_node)

workflow.set_entry_point("ingest_event")
workflow.add_edge("ingest_event", "correlate_alert")
workflow.add_edge("correlate_alert", "collect_context")
workflow.add_edge("collect_context", "retrieve_cases")
workflow.add_edge("retrieve_cases", "diagnose_rca")
workflow.add_conditional_edges(
    "diagnose_rca",
    route_after_diagnosis,
    {
        "circuit_break": "analyze_blast_radius",
        "heal": "plan_heal",
        "escalate": "escalate_human",
    },
)
workflow.add_edge("analyze_blast_radius", "circuit_break")
workflow.add_edge("circuit_break", "notify_owners")
workflow.add_edge("plan_heal", "guardrail_check")
workflow.add_conditional_edges(
    "guardrail_check",
    route_after_guardrail,
    {
        "allow": "execute_heal",
        "require_approval": "escalate_human",
        "block": "escalate_human",
    },
)
workflow.add_edge("execute_heal", "verify_recovery")
workflow.add_conditional_edges(
    "verify_recovery",
    route_after_verification,
    {
        "success": "notify_owners",
        "failed": "escalate_human",
    },
)
workflow.add_edge("notify_owners", "respond")
workflow.add_edge("escalate_human", "respond")
workflow.add_edge("respond", END)

graph = workflow.compile()
```

---

## 5. API 接口设计

### 5.1 REST API（FastAPI）

#### 告警接入接口

```text
POST /api/v1/incidents/ingest
```

请求体：

```json
{
  "source": "spark",
  "auto_execute": false,
  "payload": {
    "cluster": "dw-prod",
    "job_name": "dws_user_growth_nightly",
    "application_id": "application_1714858800_1024",
    "severity": "P0",
    "message": "ExecutorLostFailure: Remote RPC client disassociated. Container killed by YARN for exceeding memory limits"
  }
}
```

响应体（200）：

```json
{
  "incident_id": "inc_20260504_0001",
  "status": "diagnosed",
  "incident_type": "spark_oom",
  "severity": "P0",
  "diagnosis_summary": "Spark Executor 因倾斜分区过大触发 OOM，建议提高 executor memory 并增加 shuffle partitions",
  "recommended_action": "tune_and_retry",
  "approval_required": true,
  "execution_result": null,
  "error": null
}
```

#### 事件详情与列表

```text
GET /api/v1/incidents
GET /api/v1/incidents/{incident_id}
```

#### 人工确认与动作执行

```text
POST /api/v1/incidents/{incident_id}/approve
POST /api/v1/incidents/{incident_id}/execute
POST /api/v1/incidents/{incident_id}/replay
```

`POST /api/v1/incidents/{incident_id}/approve` 请求体：

```json
{
  "incident_id": "inc_20260504_0001",
  "approver": "oncall_data_engineer",
  "approved": true,
  "comment": "允许执行一次受控重试"
}
```

#### 知识库与元数据同步

```text
POST /api/v1/knowledge/sync          # 同步历史 incident、runbook、修复记录
POST /api/v1/catalog/sync             # 同步表元数据、owner、血缘信息
GET  /api/v1/knowledge/cases/{id}     # 查看历史案例详情
```

#### 健康检查

```text
GET /api/v1/health
```

```json
{
  "status": "ok",
  "connectors_connected": ["spark_history", "flink_api", "scheduler", "catalog"],
  "vector_store_count": 312,
  "pending_incidents": 1
}
```

### 5.2 LLM API 调用约定

所有 LLM 调用统一走 OpenAI 兼容接口，并通过 Instructor 强制输出结构化诊断结果与动作计划：

```python
import instructor
from openai import OpenAI

client = instructor.from_openai(
    OpenAI(base_url=settings.llm_base_url, api_key=settings.llm_api_key)
)

diagnosis = client.chat.completions.create(
    model=settings.llm_model,
    response_model=RootCauseDiagnosis,
    messages=[
        {"role": "system", "content": rendered_system_prompt},
        {"role": "user", "content": incident_context_text},
    ],
    temperature=0,
)
```

### 5.3 FastMCP Tool 调用约定

NightWatch 不直接在 Agent 节点中硬编码调用外部系统，而是通过 FastMCP 暴露标准工具：

```python
from fastmcp import FastMCP

mcp = FastMCP("nightwatch-control-plane")


@mcp.tool()
def retry_scheduler_task(task_id: str, parameters: dict) -> dict:
    """对调度器任务执行受控重试。"""
    return scheduler_connector.retry_task(task_id=task_id, parameters=parameters)


@mcp.tool()
def pause_flink_job(job_id: str, reason: str) -> dict:
    """对高风险 CDC 作业执行物理熔断。"""
    return flink_connector.pause_job(job_id=job_id, reason=reason)


@mcp.tool()
def create_github_issue(repo: str, title: str, body: str, assignees: list[str]) -> dict:
    """创建带证据链的 GitHub Issue。"""
    return github_connector.create_issue(repo=repo, title=title, body=body, assignees=assignees)
```

工具约束：

- 所有工具必须在 `registry.py` 中声明权限等级与可用环境。
- 生产环境工具默认只暴露白名单动作，不允许任意 Shell 或 SQL 执行。

### 5.4 向量库调用约定

```python
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

qdrant = QdrantClient(path="./qdrant_data")

qdrant.create_collection(
    collection_name="incident_memory",
    vectors_config=VectorParams(size=1024, distance=Distance.COSINE),
)

qdrant.upsert(
    collection_name="incident_memory",
    points=[
        PointStruct(
            id=incident_hash,
            vector=embedding,
            payload={
                "incident_type": diagnosis.incident_type,
                "error_signature": incident.error_signature,
                "summary": diagnosis.summary,
                "resolution": resolved_action,
                "cluster": incident.cluster,
            },
        )
    ],
)

results = qdrant.search(
    collection_name="incident_memory",
    query_vector=query_embedding,
    limit=8,
)
```

---

## 6. 配置管理

### 6.1 全局配置（`config.py`）

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # LLM
    llm_provider: str = "deepseek"
    llm_api_key: str
    llm_base_url: str = "https://api.deepseek.com"
    llm_model: str = "deepseek-chat"
    llm_temperature: float = 0.0

    # 向量库
    qdrant_path: str = "./qdrant_data"
    embedding_model: str = "BAAI/bge-large-zh-v1.5"
    retrieval_top_k: int = 8

    # Spark / YARN
    yarn_rm_base_url: str = "http://localhost:8088"
    spark_history_base_url: str = "http://localhost:18080"

    # Flink
    flink_rest_base_url: str = "http://localhost:8081"
    flink_default_cluster: str = "realtime-prod"

    # Catalog / Lineage
    catalog_api_base_url: str = "http://localhost:8181"
    paimon_warehouse: str = "s3://warehouse/paimon"
    starrocks_jdbc_url: str = "jdbc:mysql://localhost:9030"

    # Scheduler
    scheduler_type: str = "dolphinscheduler"     # airflow / dolphinscheduler
    scheduler_api_base_url: str = "http://localhost:12345"

    # 协同平台
    github_token: str = ""
    github_repo: str = ""
    jira_base_url: str = ""
    jira_token: str = ""
    feishu_webhook: str = ""

    # 安全与重试
    auto_retry_limit: int = 2
    approval_required_by_default: bool = True
    incident_cooldown_seconds: int = 600

    # 可观测性
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    langfuse_host: str = "http://localhost:3000"

    # Server
    server_host: str = "0.0.0.0"
    server_port: int = 8000
    mcp_host: str = "0.0.0.0"
    mcp_port: int = 8081

    class Config:
        env_file = ".env"
        env_prefix = "NW_"
```

### 6.2 Prompt 管理策略

**MVP 阶段**：所有 Prompt 以 Python 常量形式写在 `agent/prompts.py` 中。理由：

- 夜间事件处理链路长，先减少抽象层，保证调试路径最短。
- RCA、修复规划、通知草稿三类 Prompt 在早期会快速迭代，常量更利于联调。
- 状态机跑通后，再迁移到 Jinja2 + YAML 模板体系。

```python
SYSTEM_DIAGNOSE_INCIDENT = """
你是 NightWatch 的夜间值守诊断器。你的任务不是泛泛分析，而是基于日志、指标和历史案例给出可执行的根因判断。

规则：
1. 必须优先输出最可能的单一根因，不要泛化为多个模糊猜测
2. 必须引用日志片段、指标峰值或历史案例作为证据
3. 如果发现 Schema Drift 或数据污染风险，必须标记 requires_circuit_break=true
4. 不要输出未在上下文中出现的任务名、库表名或系统组件
"""

SYSTEM_PLAN_HEAL = """
你是 NightWatch 的自动修复规划器。只能在允许的目标系统和参数范围内生成修复计划。

规则：
1. 仅输出单个最优动作
2. 若风险高于 medium，默认 requires_approval=true
3. 对 Schema Drift、未知 DDL、脏数据扩散场景，不允许输出 tune_and_retry
"""
```

**生产阶段迁移**：将 Prompt 迁移至 `prompts/` 目录并引入版本管理、A/B 测试与灰度发布。节点代码保持不变，仅替换 Prompt 渲染入口。

---

## 7. 安全设计

安全设计要点如下：

- 自动化动作越权：通过动作白名单控制；`guardrails.py` 只允许调参、重试、暂停、隔离等受控动作，禁止任意命令执行。
- 参数失控：通过上下界校验控制；用 Pydantic 模型约束 Spark / Flink 参数，超范围直接阻断。
- 重试风暴：通过限次 + 冷却控制；每个 incident 最多自动尝试 2 次，并设置 cooldown 窗口。
- Schema Drift 污染扩散：通过熔断优先控制；识别到高风险 DDL 或不兼容类型时，默认暂停 CDC 与下游 DAG。
- MCP 工具误用：通过最小权限控制；FastMCP 工具按环境、权限等级和操作类型注册，不暴露通用高危接口。
- 日志敏感信息泄露：通过脱敏控制；在日志切片阶段遮蔽账号、手机号、token、JDBC 凭据等敏感内容。
- 工单误伤：通过置信度门槛控制；低置信度诊断不自动提单给个人，仅发组级通知或要求人工确认。
- 审计缺失：通过追加式审计记录控制；所有诊断输入、动作计划、执行结果、通知对象都写入审计流水。

---

## 8. 依赖清单（`requirements.txt`）

```text
# Agent & LLM
langgraph>=0.2
langchain-core>=0.2
langchain-openai>=0.1
instructor>=1.0
pydantic>=2.0
pydantic-settings>=2.0

# MCP
fastmcp>=2.0

# Retrieval
qdrant-client>=1.9
sentence-transformers>=2.2
rank-bm25>=0.2

# Web & Worker
fastapi>=0.110
uvicorn>=0.29
gradio>=4.0
httpx>=0.27
tenacity>=8.2

# Observability & Logging
langfuse>=2.0
prometheus-client>=0.20
structlog>=24.0
orjson>=3.10

# Integration
PyGithub>=2.3
jira>=3.8
pyyaml>=6.0
jinja2>=3.0

# Dev & Test
pytest>=8.0
pytest-asyncio>=0.23
faker>=24.0
respx>=0.21
```

---

## 9. 测试规范（TDD）

### 9.1 原则

**所有核心节点与护栏函数必须先写 Pytest 测试，再实现功能代码。** NightWatch 的价值不在“会说”，而在“夜里真的敢执行”。因此所有 RCA、动作白名单、熔断决策都必须有可回归测试覆盖。

### 9.2 测试目录结构

```text
tests/
├── conftest.py                  # mock Spark/Flink connector、样例 incident、mock LLM
├── unit/
│   ├── test_slicer.py           # 日志切片：时间窗口、错误签名、摘要质量
│   ├── test_correlator.py       # 告警聚合：重复事件合并、签名归一化
│   ├── test_spark_rca.py        # Spark 诊断：OOM / skew / driver contention 分类
│   ├── test_flink_rca.py        # Flink 诊断：checkpoint 超时 / 背压 / CDC DDL 漂移
│   ├── test_guardrails.py       # 动作护栏：白名单、参数上下界、审批要求
│   ├── test_blast_radius.py     # 血缘影响：列级影响、owner 路由、看板命中
│   └── test_state.py            # AgentState 序列化、默认值、状态流转
├── integration/
│   ├── test_agent_graph.py      # LangGraph happy path / 熔断路径 / 人工升级路径
│   ├── test_mcp_tools.py        # FastMCP 工具调用、权限限制、异常回传
│   └── test_api.py              # FastAPI 告警接入、审批执行、健康检查
└── fixtures/
    ├── spark_oom_logs.txt
    ├── spark_driver_contention.txt
    ├── flink_cdc_drift.json
    ├── paimon_write_stall.json
    └── lineage_graph.json
```

### 9.3 关键 Fixture

```python
# tests/conftest.py
import pytest
from datetime import datetime

from nightwatch.models.incident import IncidentEvent, IncidentType


@pytest.fixture
def sample_spark_oom_incident() -> IncidentEvent:
    return IncidentEvent(
        incident_id="inc_test_001",
        source="spark",
        incident_type=IncidentType.SPARK_OOM,
        severity="P0",
        cluster="dw-prod",
        job_name="dws_user_growth_nightly",
        application_id="application_1714858800_1024",
        event_time=datetime.utcnow(),
        error_signature="Container killed by YARN for exceeding memory limits",
    )


@pytest.fixture
def mock_connectors(mocker):
    connectors = mocker.MagicMock()
    connectors.spark.get_executor_metrics.return_value = {
        "executor_lost_count": 3,
        "max_shuffle_read_mb": 9216,
    }
    connectors.scheduler.retry_task.return_value = {"status": "submitted"}
    return connectors


@pytest.fixture
def mock_llm():
    # 预置 RootCauseDiagnosis / HealActionPlan 结构化返回
    ...
```

### 9.4 测试优先级

| 优先级 | 模块 | 必须覆盖的场景 |
| ------ | ---- | -------------- |
| P0 | `test_spark_rca.py` | Executor OOM、数据倾斜、Driver 争抢三类场景必须可区分 |
| P0 | `test_guardrails.py` | 超范围参数阻断、Schema Drift 禁止自动重试、高风险事件要求审批 |
| P0 | `test_agent_graph.py` | 自动修复成功路径、熔断路径、失败升级人工路径 |
| P1 | `test_blast_radius.py` | 上游字段删除后能正确命中下游表、看板与 owner |
| P1 | `test_mcp_tools.py` | MCP 工具权限隔离、调用失败时返回结构化错误 |
| P2 | `test_api.py` | 告警接入 200、审批拒绝、健康检查响应结构 |

---

## 10. 部署架构（MVP 单机 Docker + Background Worker）

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY nightwatch/ ./nightwatch/
EXPOSE 8000 8081 7860
CMD ["sh", "-c", "uvicorn nightwatch.main:app --host 0.0.0.0 --port 8000 & python -m nightwatch.worker & python -m nightwatch.ui.app"]
```

```text
┌───────────────────────────────────────────────┐
│               Docker Container                │
│                                               │
│  :8000  FastAPI REST API                      │
│  :8081  FastMCP Control Plane                 │
│  :7860  Gradio Oncall Console                 │
│                                               │
│  Background Worker                            │
│  - Incident 消费与 LangGraph 执行             │
│  - 自动恢复 / 熔断 / 通知                     │
│                                               │
│  ./qdrant_data/     历史案例向量库            │
│  ./audit_logs/      审计流水                  │
│  ./.env             配置与凭据                │
└───────────────────┬───────────────────────────┘
                    │ HTTP / REST / Webhook / MCP
                    ▼
      Spark / YARN / Flink / Scheduler / Catalog / GitHub / Jira
```

部署原则：

- MVP 阶段以单容器打通完整链路，先验证夜间诊断、修复、熔断和通知闭环。
- 生产阶段拆分为 API、worker、MCP control plane、vector store 四类组件，分别扩容与隔离权限。
- 所有外部系统凭据统一通过环境变量与密钥管理服务注入，不写入代码仓库。
