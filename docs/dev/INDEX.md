# 开发计划执行索引

本文件此前缺失；根据项目约定从 PLAN-013 起开始维护。更早计划和结果文件仍保留在 `docs/dev/` 目录，可通过文件名追溯。

## 2026-07-09 08:17

- 计划文件：`PLAN-013-review-fixes.md`
- 结果文件：`PLAN-013-review-fixes-OUTCOME.md`
- 摘要：修复系统 review 发现的过期任务重复投递、依赖声明缺失、旧历史格式统计报错、队列发现过宽、legacy recovery 默认边界和运行时版本号不一致问题。

## 2026-07-09 08:21

- 计划文件：`PLAN-013-review-fixes.md`
- 结果文件：`PLAN-013-review-fixes-OUTCOME.md`
- 摘要：已完成代码、测试和依赖元数据修复；全量测试 94 passed，ruff/mypy 通过，临时 venv 最小安装导入通过。本轮未修改用户已有的 `AGENTS.md` 工作树变更。

## 2026-09-20 14:05

- 计划文件：`PLAN-014-review-fixes.md`
- 结果文件：`PLAN-014-review-fixes-OUTCOME.md`
- 摘要：落实三轮评审（qtask-design-review-20260920-scenarios / review-20260920-design-flaws / REVIEW-20260920-stock-data-ingestion）合并修复：logical_key 业务身份去重、重试指数退避、expire 执行截止强制（skipped 状态）、zstd 线程局部化、外存错误分类、归档仅 terminal、维护线程周期 stale recovery、DLQ 重放重置重试计数、push_batch 补参数、stop 卡死修复；另修复实施中发现的 maintenance 初始化竞态等 3 个新问题。测试 114 passed（新增 22），ruff/mypy 通过，端到端三场景验证通过。配套设计定稿 `docs/design/task-identity-20260920-logical-key.md`。本轮未提交用户已有的 `AGENTS.md` 工作树变更。

## 2026-09-20 22:08

- 计划文件：`PLAN-015-qtask-v2.md`
- 结果文件：`PLAN-015-qtask-v2-OUTCOME.md`
- 摘要：完成 V2 核心实现、入口统一、状态机与 Dashboard/前端交付的最终收尾；PLAN-016 补齐旧 V1 双读、测试迁移、文档和示例，形成可验证的整体结果。本轮继续排除用户已有的 `AGENTS.md` 修改。

## 2026-09-20 22:09

- 计划文件：`PLAN-016-v2-review-remediation.md`
- 结果文件：`PLAN-016-v2-review-remediation-OUTCOME.md`
- 摘要：修复 V1/V2 管理兼容、retry_wait 注入统计、REST 4xx、Worker 优雅停止信号和前端计数；迁移全量测试与手工脚本，新增 TaskSpec 调度、新闻 fan-out、Reconciler 示例，并同步 README/skill。验证与提交推送结果见结果文件。

## 2026-09-21 17:05

- 计划文件：`PLAN-017-dashboard-react.md`
- 结果文件：`PLAN-017-dashboard-react-OUTCOME.md`
- 摘要：按 `docs/ui-design/v1/` 设计稿实施 React 版管理后台（frontend/，Vite+React+TS，构建产物入库 dashboard/static/spa/）：总览进度矩阵、队列列表/详情、全局任务视图+详情抽屉、Worker 监控、告警中心（前端规则引擎+localStorage）、教程页、双主题；后端增量 SPA 托管与 clean-history 接口，退役旧 Jinja+原生 JS 单页。pytest 118 passed，ruff/mypy/build 通过，浏览器端到端验证后测试数据已清理。

## 2026-09-21 22:30

- 计划文件：`PLAN-018-dashboard-spa-auth.md`
- 结果文件：`PLAN-018-dashboard-spa-auth-OUTCOME.md`
- 摘要：核查确认登录系统（PLAN-004）已满足"默认免登录、设 QTASK_DASHBOARD_PASSWORD 强制登录"；补齐 PLAN-017 引入的 SPA 路由回退鉴权缺口（spa_fallback 未登录 307 跳 /login），新增测试与 README 说明。pytest 119 passed，ruff/mypy 通过。

## 2026-09-21 23:40

- 计划文件：`PLAN-019-root-consolidation-pnpm.md`
- 结果文件：`PLAN-019-root-consolidation-pnpm-OUTCOME.md`
- 摘要：仓库收敛为单包结构（cli/dashboard/remote_storage/frontend 并入 qtask_list/，根目录 15 项→9 项）；删除冗余 requirements.txt；前端切换 pnpm（pnpm-lock.yaml 入库、packageManager 字段、esbuild 构建白名单）；同步 import/入口点/打包配置/示例模块路径/README/skills/HANDOFF。pytest 119 passed，ruff/mypy/pnpm build/uvicorn 登录冒烟通过。

## 2026-09-29 08:16

- 计划文件：`PLAN-020-review-remediation.md`
- 结果文件：`PLAN-020-review-remediation-OUTCOME.md`
- 摘要：修复全代码设计评审的 Worker 心跳与领取后租约、RemoteStorage 安全与终态回收、活跃任务管理删除、总览查询成本、跨队列游标搜索、前端剩余量和速率、公开 Dashboard 启动路径；补充旧队列指标回填与批量投递部分成功契约。Python 编译、ruff、mypy、前端构建和 diff 检查通过；本次未执行运行时测试。

## 2026-09-29 14:19

- 计划文件：`PLAN-021-review-followup.md`
- 结果文件：`PLAN-021-review-followup-OUTCOME.md`
- 摘要：复查并修复 Worker 停机领取/handler 准入竞态、外存 pending 回收与手动删除中断窗口、跨队列游标队列快照、队列详情稀疏筛选无界扫描和旧队列缓存；更新测试基线、README、项目使用指南与前端分页交互。隔离 Redis 全量测试 129 passed，ruff、mypy、pnpm build、diff 检查通过。

## 2026-09-29 21:03

- 计划文件：`PLAN-022-cli-dashboard-operations.md`
- 结果文件：`PLAN-022-cli-dashboard-operations-OUTCOME.md`
- 设计文件：`docs/design/cli-dashboard-20260929-operations-review.md`
- 摘要：从值班巡检、跨队列排查、无人消费、过期重放、故障显示和远程访问审查 CLI 与 Dashboard；新增 CLI 异常/JSON 巡检和远程认证保护，修正采样进度口径、告警、全局筛选、刷新、投递及过期任务操作。隔离 Redis 全量测试 136 passed，ruff、mypy、pnpm build 与浏览器场景验证通过。

## 2026-10-16 07:37

- 计划文件：`PLAN-023-all-extra.md`
- 结果文件：`PLAN-023-all-extra-OUTCOME.md`
- 设计文件：`docs/design/install-extras-20261016-all.md`
- 摘要：增加 `all` 安装 extra，聚合 dev、dashboard、storage 的可选依赖去重并集；README 补充一键安装命令并说明包含开发工具。

## 2026-10-16 08:25

- 计划文件：`PLAN-024-core-reliability.md`
- 结果文件：`PLAN-024-core-reliability-OUTCOME.md`
- 设计文件：`docs/design/core-reliability-20261016-remediation.md`
- 摘要：修复非 UI 核心审查确认的 11 项问题，并经三轮修复复查补齐响应丢失预算、历史文本与在线格式转换、指标预检、命名边界、owner 迁移与保留、恢复索引及心跳竞争、时钟偏差和外存失败预算。增加临时 Redis 与文件目录隔离、70 项回归；全量 206 passed，ruff、mypy、50 个 Python 源文件编译和 diff 检查通过。README 同步接口与升级边界。
