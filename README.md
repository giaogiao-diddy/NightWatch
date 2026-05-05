# NightWatch (守夜人)

> 面向实时湖仓场景的夜间值守与自愈 Agent。  
> 在无人值守窗口内完成告警归一化、根因诊断、风险护栏、自动修复、恢复验证与责任路由。

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![LangGraph](https://img.shields.io/badge/LangGraph-Stateful%20Agent-111111)](https://github.com/langchain-ai/langgraph)
[![Next.js](https://img.shields.io/badge/Next.js-16-000000?logo=nextdotjs&logoColor=white)](https://nextjs.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

---

## 为什么是 NightWatch

NightWatch 聚焦一个最难但最有价值的生产问题：  
**在凌晨 2 点，数据平台出现高危故障时，系统是否能在“可控风险”内自动完成诊断与修复，而不是只会报警。**

它不是简单的“LLM 调 API 脚本”，而是一个带状态机、审计、护栏、执行边界和可观测面的生产级智能控制平面：

- **Schema Drift 物理熔断**：识别 Flink CDC 字段漂移后，禁止自动重试，直接进入隔离与下游阻断链路。
- **大脑与触手彻底解耦**：LangGraph 只做决策编排；真实执行统一经 FastMCP 工具边界下发。
- **Try-Heal-Retry 闭环**：诊断 -> 规划 -> 护栏 -> 执行 -> 验证 -> 升级/继续，链路可追溯。
- **混合检索增强 RCA**：BM25 + 向量检索融合，提高历史 case 召回质量与解释性。
- **企业级可观测与运维**：Prometheus + Grafana + API/MCP/Worker 三平面部署。

---

## 系统架构

```mermaid
flowchart LR
    A[Webhook / Alert Source\nAirflow Spark Flink] --> B[FastAPI Ingest API]
    B --> C[Incident Normalizer + Correlator]
    C --> D[LangGraph Orchestrator]

    subgraph G[LangGraph State Machine]
      D1[ingest_alert]
      D2[retrieve_cases\nBM25 + Vector]
      D3[diagnose_rca]
      D4[plan_heal]
      D5[guardrail_check]
      D6[human_approval]
      D7[execute_heal]
      D8[verify_recovery]
      D9[circuit_break]
      D10[notify_owners]
      D1 --> D2 --> D3 --> D4 --> D5
      D5 -->|allow| D7 --> D8
      D5 -->|require approval| D6 --> D7
      D5 -->|block| D9
      D8 --> D10
      D9 --> D10
    end

    D --> H[(SQLite Checkpointer\nRuntime Store)]
    D2 --> Q[(Qdrant Memory)]

    D7 --> M[FastMCP Server\nTool Gateway]
    D9 --> M
    D10 --> M

    subgraph E[Execution Engines / External Systems]
      E1[Spark Control Plane]
      E2[Scheduler API]
      E3[Flink CDC API]
      E4[GitHub / Jira]
    end

    M --> E1
    M --> E2
    M --> E3
    M --> E4

    B --> P[/metrics]
    P --> O[Prometheus + Grafana]

    F[Next.js Console] --> B
    F --> P
```

---

## 核心特性

### 1) 面向生产故障的强约束自愈
- Spark OOM / Skew：支持参数调优后受控重试。
- Flink Schema Drift：直接高风险处理，触发熔断与下游防扩散。
- 高风险策略：风险等级高时自动进入审批门，不绕过人工确认。

### 2) “决策层”与“执行层”物理隔离
- LangGraph 节点只负责编排，不直接请求外部系统。
- 所有执行动作经过 FastMCP 工具注册与角色权限控制。
- 执行动作可审计、可回放，便于责任追踪。

### 3) 混合检索驱动的证据化诊断
- 词法召回：BM25 对结构化错误签名敏感。
- 语义召回：Qdrant 向量搜索补足同义表达与上下文差异。
- 融合排序：双路评分融合，提升召回稳定性。

### 4) 端到端工程化
- API、Worker、MCP Server、Qdrant、Prometheus、Grafana 一体化部署。
- Next.js 企业控制台支持事件演练、配置管理、状态观测。
- 核心测试全绿（当前为 22 项核心测试场景）。

---

## 技术栈

### 后端与 Agent
- Python 3.11+
- FastAPI
- LangGraph + SQLite Checkpointer
- Pydantic v2 / pydantic-settings
- Instructor + OpenAI SDK（结构化 LLM 输出）
- FastMCP

### 数据与检索
- Qdrant
- rank-bm25

### 前端
- Next.js 16
- React 19
- TypeScript
- Tailwind CSS

### 基础设施
- Docker / Docker Compose
- Prometheus / Grafana

### 测试
- pytest
- pytest-asyncio

---

## 快速开始（Quick Start）

### 方式 A：一键生产形态（推荐）

#### 1. 准备环境变量
复制示例配置并按需修改：

```bash
cp .env.example .env
```

至少确保这些变量已设置：
- `NW_API_AUTH_KEY`
- `NW_LLM_API_KEY`
- `GF_SECURITY_ADMIN_PASSWORD`

#### 2. 一键启动

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

#### 3. 访问入口
- NightWatch API: http://localhost:8000/docs
- MCP Server: http://localhost:9001
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

#### 4. 停止服务

```bash
docker compose -f docker-compose.prod.yml down
```

---

### 方式 B：本地开发形态

#### 1. 安装后端依赖

```bash
pip install -r requirements.txt
```

#### 2. 启动 API

```bash
uvicorn nightwatch.main:app --reload --port 8000
```

#### 3. 启动 Worker

```bash
python -m nightwatch.worker
```

#### 4. 启动 MCP Server

```bash
python -m nightwatch.mcp.server
```

#### 5. 启动前端

```bash
npm install
npm run dev
```

---

## 常用接口

- `POST /api/v1/incidents/ingest`：接入并处理告警事件
- `GET /api/v1/runtime-config`：读取运行时连接器配置
- `PUT /api/v1/runtime-config`：更新运行时连接器配置
- `GET /metrics`：Prometheus 指标输出

> 注意：`/api/v1/runtime-config` 为运行时配置，默认不持久化到重启后。

---

## 仓库结构

```text
nightwatch/
├── main.py
├── worker.py
├── config.py
├── api/
├── agent/
├── ingest/
├── retrieval/
├── diagnostics/
├── healing/
├── lineage/
├── connectors/
└── mcp/
app/
├── page.tsx
└── ...
```

---

## 安全与合规

- 默认支持 API Key 认证（`X-API-Key` 或 `Authorization: Bearer`）。
- 所有高风险动作都必须经过护栏判定。
- 建议在线上启用 TLS、最小权限凭据、敏感信息脱敏和审计保留。

详见 [SECURITY.md](SECURITY.md)。

---

## 贡献指南

欢迎 issue / PR / 架构讨论。  
开始贡献前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。

---

## License

Apache-2.0，详见 [LICENSE](LICENSE)。
