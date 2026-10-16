# 自定义 processing 统计与恢复计划

- 创建时间：2026-10-17 00:07（Asia/Shanghai）。
- 创建模型：Codex，环境声明基于 GPT-6；具体模型 ID 未提供，记为 model-unknown。
- 分析范围：WORKTREE；基准提交 `3ff689706e788264b02839c4e2db9bd8a4644d50`，分支 `main`。内容尚未固定为提交。
- 初始相关未提交或未跟踪文件：`qtask_list/admin.py`、`qtask_list/state.py`、`qtask_list/worker.py`、`docs/HANDOFF.md`、`docs/known-issues/KI-20261016-2343-001-custom-processing-index.md`、`tests/test_registry_discovery.py` 及既有 Dashboard 前端与静态产物。它们来自此前工作，本计划要求保留。
- 关联问题：[KI-20261016-2343-001](../known-issues/KI-20261016-2343-001-custom-processing-index.md)。

## 目标、范围与边界

修复公开 `SmartQueue(processing_key=...)` 消费路径：领取到自定义 processing List 的 V2 任务后，管理端统计、processing 查看和安全恢复都能找到该 List。

范围包括内部 Redis processing-key 登记、`SmartQueue` 领取路径、管理端/队列的 key 枚举、队列删除清理和回归测试。保持现有构造参数与 Worker API，不把自定义消费者伪装成 Worker，不恢复队列或 Worker 的全库发现扫描。登记按队列持久保存，在删除该逻辑队列时一并清除。

本次不改变任务状态机、公开方法签名、V1 消息兼容策略、Worker 心跳语义、Dashboard 页面布局，也不提交或推送。

## 当前状态、依据与约束

- 当前 `QueueAdmin.queue_stats()` 只统计已登记 Worker 的 processing key 和默认 `<queue>:processing`，遗漏无 Worker 心跳的自定义 key。
- 当前 `QueueAdmin.processing_keys()` 与 `SmartQueue.processing_keys()` 通过按队列 `SCAN` 枚举 `:processing:*`。队列自动发现已明确以 `qtask:queues` 和 `qtask:workers:<queue>` 为唯一来源；本修复需为当前消费者补登记，不能把全局扫描重新用于发现。
- 现有复现已确认：自定义 List 长度和 `SmartQueue.get_stats()["processing"]` 为 1，`QueueAdmin.queue_stats()["processing"]` 为 0；详情见问题条目引用的隔离 Redis 记录。
- 公开 `processing_key` 可以是自定义名称。登记必须先于阻塞或非阻塞领取，避免消息移入 processing 与登记之间进程退出造成的漏项。

## 方案比较与选择

1. 每次管理查询按队列扫描 Redis：改动较少，但把登记缺失隐藏为运行时扫描，管理热路径持续扫描键空间，也不符合本轮“完善当前数据登记”的约束。
2. 新增每队列 processing-key ZSET：自定义 `SmartQueue` 在领取前原子登记 key；现有 Worker 仍由 Worker 注册表提供 key。统计、查看、恢复和清理只读确定的注册表和默认 key。这不改变用户接口，且登记与领取之间没有状态窗口，因此采用此方案。
3. 把自定义消费者放入 Worker 注册表：会错误表示它拥有 Worker 心跳和生命周期，也会让恢复安全判断失真，因此不采用。

登记成员在队列生命周期内保留，以免空队列检查和并发领取之间的清理竞争；同一个 key 的重复登记幂等。队列删除时清除索引。

## 实施步骤

1. 在 `SmartQueue` 增加每队列 processing-key 注册表标识与领取前登记；已有 Worker key 继续使用 Worker 注册表，不重复伪造 Worker 状态。
2. 将 `SmartQueue` 和 `QueueAdmin` 的 processing-key 枚举改为合并自定义 key 注册表、Worker 注册表和默认 processing key；让管理统计、peek、recover、clear 等已有调用共用该枚举，并在彻底删除队列时清除登记索引。
3. 添加隔离 Redis 回归：自定义 key 领取后统计为 1；ack/fail 结算后计数归零；消费者无 heartbeat 时退出/故障后可由 QueueAdmin recover；正常 Worker 与默认 processing 保持现有行为；键类型错误不产生部分登记。
4. 更新 README、问题详情和交接摘要，记录新的登记与恢复边界。

## 自查、验证与完成标准

实施前自查：索引必须在 `BRPOPLPUSH`/`RPOPLPUSH` 前登记；不能把空 key 的修剪与并发领取竞争；Worker-specific key 仍由既有注册表支撑；队列清理必须删除索引；`include_legacy=False` 仍排除默认 processing，但保留已登记的专属 processing key。

验证运行新增定向回归、`uv run --no-project python -m pytest tests/ -q`、`pnpm test`、`uv run --no-project python -m ruff check .`、`uv run --no-project python -m mypy qtask_list` 和 `git diff --check`。若修改实际 Dashboard API 路径，再用隔离 Redis 验证 `/api/queues` processing 计数。

完成标准：自定义 V2 任务进入 processing 后，QueueAdmin 和 Dashboard API 计数正确；ack、fail 后不再计入 processing；消费者退出或故障后可找到并恢复任务；Worker/default 行为不回归；相关验证通过；问题详情追加解决事件并归档。

## 主要风险与回滚

- 新索引需要与领取路径保持一致；登记在领取前并通过 Lua 写入，避免 Lua 键类型冲突造成部分登记。
- 索引成员保留到队列删除，动态生成大量不同 processing key 的调用方会增加少量 Redis 元数据；稳定使用一个 key 时成员不增长。
- 严格使用注册表后，修复前已存在且从未登记的自定义 processing key 不会自动进入新索引；不为此恢复全局或按键空间扫描。可通过调用方使用该 key 再次领取完成登记；现有 Worker key 仍由 Worker 注册表发现。
- 若实现或验证出现问题，只回退本次新增处理路径、注册表读取和对应测试/文档；保留工作区在本任务前已有的改动，不重置整文件。
