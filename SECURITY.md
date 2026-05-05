# Security Policy

## Supported Versions

NightWatch 当前以主分支和最新发布版本为主进行安全维护。  
请优先使用最新版本以获取安全修复。

## Reporting a Vulnerability

如果你发现潜在安全漏洞，请不要公开提交 issue。

建议流程：

1. 通过私密渠道联系维护者（邮件或私有安全通道）。
2. 提供最小复现信息：影响范围、触发条件、PoC（如可提供）。
3. 维护者将在 72 小时内确认并给出初步响应。
4. 修复完成并发布后，再协商公开披露细节。

## Security Baseline (Recommended)

### 1) 凭据管理
- 禁止将真实密钥提交到仓库。
- 使用 Secret Manager 或 CI Secret 注入。
- 定期轮转 `NW_API_AUTH_KEY`、`NW_LLM_API_KEY`、`NW_QDRANT_API_KEY`。

### 2) 网络与访问控制
- 线上环境启用 TLS。
- MCP Server 不暴露在公网，置于内网或 API 网关之后。
- 使用最小权限连接器账号。

### 3) 动作安全
- 所有自愈动作必须经过 guardrail 判定。
- 高风险事件必须 require_approval。
- Schema Drift 等高危场景禁止自动重试。

### 4) 数据安全
- 事故日志中敏感字段应脱敏。
- 审计日志保留访问控制与生命周期策略。

### 5) 供应链安全
- 固化依赖版本并开启 CI 安全扫描。
- 对外部镜像使用可信来源并定期更新。

## Known Security Trade-offs

- 默认 demo 模式允许在未配置 API key 时降低鉴权门槛，便于本地体验。  
  生产环境必须配置 `NW_API_AUTH_KEY` 并关闭 `NW_CONNECTORS_DEMO_FALLBACK`。

感谢你帮助 NightWatch 变得更安全。
