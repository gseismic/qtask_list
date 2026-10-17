# 注册表统一发现结果

- 创建时间：2026-10-16 18:58（Asia/Shanghai）。
- 实际执行模型：Codex，环境声明基于 GPT-6；未提供具体模型 ID，记为 model-unknown。
- 版本：WORKTREE，基准提交 3ff689706e788264b02839c4e2db9bd8a4644d50，尚未提交。
- 关联计划：[注册表统一发现](./PLAN-20261016-002-model-unknown-registry-discovery.md)。
- 状态：已完成。

## 完成内容与理由

- qtask_list/admin.py 的 queue_names 直接读取 qtask:queues；list_workers 使用对应的 Worker 注册表。已删除 V1 自动发现、小时缓存、上一轮缓存失效补丁和专用消息探测助手。外部注册项变更在下一次查询即时生效，不扫描状态键或历史来推测队列存在。
- qtask_list/state.py、worker.py 补齐登记生命周期：心跳在一个 Lua 脚本中校验 namespace、键类型和参数，原子登记队列、Worker 与心跳。空闲消费者也可见；退出有残留 processing 时保留失联 Worker 注册项，供诊断恢复。清理失效注册项时原子复核心跳与 processing，保护并发续租。
- README.md 明确注册表为唯一发现来源，说明空逻辑队列、外部注册项删除和活跃 Worker 重新登记语义。手工测试样本显式登记，不再依赖兼容扫描。
- 保留上一任务的搜索修复和生产 SPA；未改变前端产品源码，没有新增依赖。

相关测试为 tests/test_registry_discovery.py、test_dashboard_api.py、test_cli.py；隔离浏览器种子脚本为 tests/manual/handoff_dashboard_server.py。

## 调整、自查与验证

范围按计划完成。实施中补充了“清理失效注册项与并发续租交错”的原子复核和行为测试，先复现再修复。复核了 namespace 并发冲突、Redis 类型错误无部分写入、空闲/退出/失联 Worker 及外部清空恢复。

- 全量 Python：243 passed，80 条既有弃用提醒。
- 前端：15 项行为测试通过。
- 浏览器：15 项搜索回归和 3 项注册表增删/清空检查通过，0 控制台错误。
- ruff 全库、mypy 20 个模块和 git diff --check 均通过。
- 15 项旧兼容缓存测试替换为 12 项注册表/API/生命周期测试；最终注册表专项为 11 项。

环境、命令、截图和日志见[测试记录](../test/20261016-18-WORKTREE/测试记录.md)。

## 状态与交付边界

KI-20261016-1218-001 保持已解决，已在原详情追加新要求和替代实现。PLAN-20261016-001 中“保留 V1 发现”的设计及对应结果属于历史方案，队列/Worker 发现以本计划为准；其搜索修复仍有效。

本轮没有遗留已确认缺陷。没有清理业务 Redis、迁移数据、修改 AGENTS.md 或创建 GOAL.md。测试资源均已清理。现有 8766 Dashboard 未重启，需要重启加载修改。所有结果处于 WORKTREE，未提交、未推送。

## 2026-10-17 18:17（Asia/Shanghai）提交记录

- 更新者：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 当前保留的实现及相关文档已纳入提交 `a1db46ff6de872de5f115d16af85e9b264857329`，状态为已提交、未推送；被后续计划替代的设计仍按文内说明解释。
- 本文原有 WORKTREE、基准提交及测试记录保留其实际时点含义；最终整体验收见[管理后台修复验证](../test/20261017-17-WORKTREE/管理后台修复验证.md)。原有 AGENTS.md 改动未纳入提交。
