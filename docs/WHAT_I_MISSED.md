# What I Missed: NightWatch 开源盲区补充

这份清单用于回答一个关键问题：  
当技术功能都已完成后，一个项目离“高质量开源仓库”还差什么？

---

## 1) 开发者体验（DX）入口

### 你可能遗漏
没有统一命令入口，首次贡献者需要记忆大量命令。

### 已补齐
- [Makefile](../Makefile)

### 价值
- 一条命令完成安装、启动、测试、构建、部署。
- 降低协作摩擦，提高社区贡献转化率。

---

## 2) 贡献协作规范

### 你可能遗漏
缺少贡献流程、代码约束、PR 检查项，导致提交质量不稳定。

### 已补齐
- [CONTRIBUTING.md](../CONTRIBUTING.md)
- [.github/pull_request_template.md](../.github/pull_request_template.md)
- [.github/ISSUE_TEMPLATE/bug_report.md](../.github/ISSUE_TEMPLATE/bug_report.md)
- [.github/ISSUE_TEMPLATE/feature_request.md](../.github/ISSUE_TEMPLATE/feature_request.md)

### 价值
- 贡献路径清晰，审查成本更低。
- 关键规则（测试、风险、文档）可执行化。

---

## 3) 社区治理与行为边界

### 你可能遗漏
没有社区行为准则，项目规模扩大后容易出现沟通冲突。

### 已补齐
- [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md)
- [.github/CODEOWNERS](../.github/CODEOWNERS)

### 价值
- 形成可预期的社区互动规则。
- 明确关键路径代码所有权，降低维护风险。

---

## 4) 安全披露与安全基线

### 你可能遗漏
漏洞报告无通道、线上安全基线无文档，容易在开源后暴露风险。

### 已补齐
- [SECURITY.md](../SECURITY.md)

### 价值
- 建立私密漏洞披露流程。
- 固化生产安全建议（鉴权、TLS、最小权限、脱敏、供应链）。

---

## 5) 持续集成与质量门禁

### 你可能遗漏
没有自动化 CI，质量回归依赖人工执行。

### 已补齐
- [.github/workflows/ci.yml](../.github/workflows/ci.yml)

### 价值
- PR 自动跑后端测试和前端构建。
- 让质量门禁前置到合并前。

---

## 6) 依赖与供应链持续更新

### 你可能遗漏
依赖更新完全手动，安全修复滞后。

### 已补齐
- [.github/dependabot.yml](../.github/dependabot.yml)

### 价值
- 自动创建升级 PR，持续降低供应链暴露面。

---

## 7) 下一阶段建议（可选但强烈推荐）

这些是目前尚可继续增强的方向：

1. Release 管理自动化
- 建议补充 semantic-release 或 release-please，规范版本与 changelog。

2. SBOM 与镜像签名
- 建议在 CI 中引入 SBOM（CycloneDX/SPDX）和镜像签名（cosign）。

3. 真实连接器灰度验证
- 建议建立 staging 级回归套件，逐步替换 mock connector。

4. 配置持久化与回滚
- 建议将 runtime-config 接入持久化存储，并增加版本审计与一键回滚。

5. 数据脱敏审计
- 建议新增统一脱敏中间层，防止日志中出现密钥、账号、手机号等敏感数据。

---

## 总结

NightWatch 已经具备“技术可用”的产品形态。  
本轮补齐后，它同时具备了“开源可维护”的工程形态：

- 可读（README / docs）
- 可跑（Makefile / Compose）
- 可协作（Contributing / Templates）
- 可治理（Security / Codeowners / CI / Dependabot）

这也是 10k+ Star 项目真正需要的底层能力。
