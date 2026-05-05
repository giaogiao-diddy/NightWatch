# Contributing to NightWatch

感谢你对 NightWatch 的关注。  
本项目聚焦生产级 AIOps 场景，欢迎任何改进建议与代码贡献。

---

## 1. 在提 PR 之前

1. 先查看现有 issue，避免重复工作。
2. 对于架构级改动，请先发起设计讨论（issue/讨论帖）。
3. 涉及高风险执行逻辑（healing/guardrails/mcp tools）必须附带测试。

---

## 2. 本地开发环境

### 后端

```bash
pip install -r requirements.txt
```

### 前端

```bash
npm install
```

### 启动方式

```bash
# API
uvicorn nightwatch.main:app --reload --port 8000

# Worker
python -m nightwatch.worker

# MCP Server
python -m nightwatch.mcp.server

# Web
npm run dev
```

或使用：

```bash
python scripts/run_demo_stack.py
```

---

## 3. 代码规范

- Python 3.11+，优先完整类型注解。
- 数据模型统一使用 Pydantic v2 BaseModel。
- 默认异步优先（I/O 路径），纯计算逻辑可用同步函数。
- 导入顺序：stdlib -> third-party -> local。
- 字符串使用双引号。
- 禁止 print 调试，统一使用 logging。

---

## 4. 架构规则（必须遵守）

- api/ 不能直接调用 connectors/，必须经 agent/ 编排。
- agent/nodes.py 只做编排，不写外部系统请求细节。
- 外部副作用必须走 connectors/ 或 mcp/。
- 自动修复动作执行前必须经过 healing/guardrails.py。
- models/ 只放数据结构，不放业务决策。

---

## 5. 测试要求（TDD）

核心原则：**先写测试，再写实现。**

### 运行测试

```bash
pytest tests/ -v
```

### 仅跑单元测试

```bash
pytest tests/unit/ -v
```

### 仅跑集成测试

```bash
pytest tests/integration/ -v
```

### 约束
- LLM 调用必须 mock。
- 连接器在单元测试中必须 mock。
- 测试需覆盖：happy path + 边界 + 异常路径。

---

## 6. 分支与提交规范

### 分支命名
- feat/<feature>
- fix/<bug>
- test/<test-topic>

### Commit message

```text
type: concise message
```

type 可选：
- feat
- fix
- test
- refactor
- docs
- chore

示例：

```text
feat: add schema drift guardrail block for auto retry
```

---

## 7. Pull Request Checklist

提交 PR 前请确认：

- [ ] 代码通过测试（至少受影响模块）。
- [ ] 新增公共接口已更新对应导出与文档。
- [ ] 配置项已收口到 config.py（未硬编码）。
- [ ] 高风险动作路径已覆盖 guardrail 测试。
- [ ] 变更说明清楚，包含影响范围与回滚策略。

---

## 8. 安全报告

若发现安全漏洞，请勿公开提 issue。  
请参考 [SECURITY.md](SECURITY.md) 中的私密披露流程。

---

欢迎你一起把 NightWatch 打造成真正可落地的开源 AIOps 工程样板。
