# 自定义 processing 统计与恢复计划结果

- 结果更新时间：2026-10-17 00:49（Asia/Shanghai）。
- 实际执行模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 计划：[PLAN-20261017-001](./PLAN-20261017-001-model-unknown-custom-processing-index.md)。
- 实施版本：`054f12c25d4a248a5adf80a3c1c8e18add818458`（完整 commit）；基准提交 `3ff689706e788264b02839c4e2db9bd8a4644d50`，分支 `main`。已提交，未推送。
- 相关测试记录：[自定义 processing 修复测试](../test/20261017-00-WORKTREE/自定义-processing-修复测试.md)。

## 完成情况

已在 `SmartQueue` 的阻塞和非阻塞领取前通过 Lua 登记自定义 processing key。`QueueAdmin` 与 `SmartQueue` 的 processing key 枚举现在合并自定义 key 注册表、Worker 注册表和默认 key；因此管理统计、查看、恢复及已有清理路径都能找到自定义 List。删除队列时同时删除 processing-key 注册表。Worker key 仍由 Worker 注册表管理，自定义消费者没有被伪装成 Worker，也没有恢复 Redis 键空间扫描。

新增回归覆盖领取前登记、管理端与 Dashboard 统计、ack/fail 终态计数、消费者退出后的无扫描恢复、注册表类型冲突和队列删除清理。三个原有 CLI/Dashboard Worker 用例改为显式写入 Worker 注册表，使夹具符合当前发现契约。

## 偏差与限制

首次全量 pytest 中，3 个历史测试夹具只创建孤立的 Worker processing List，没有写 `qtask:workers:<queue>`，与当前注册表唯一发现规则不一致。补齐夹具后全量验证通过，未修改生产代码以扫描或发现孤立 key。根目录没有 `package.json`，前端测试改在 `qtask_list/frontend/` 执行。没有运行 `pnpm build`，因为本次没有修改前端源代码。

修复前已存在且未登记的自定义 processing key 不会自动回填；不需要兼容旧索引状态。调用方需在新代码下对该 key 调用领取 API 完成登记，或按独立维护流程处理已知 key。

## 验证结果

- 定向 Python 回归：9 passed，132 deselected。
- 全量工作区 Python：250 passed，80 warnings，18.79s；提交候选隔离 Python：224 passed，80 warnings，19.25s。
- 前端：15 passed。
- Ruff：通过。
- mypy：20 source files 通过。
- `git diff --check`：通过。

本次修复已提交为 `054f12c25d4a248a5adf80a3c1c8e18add818458`，未推送。工作区中此前已有的 Dashboard、前端和其他文档修改均予以保留。
