# 自定义 processing 消费任务未计入管理端统计

## 创建事件

- ID：KI-20261016-2343-001
- 标题：自定义 processing 消费任务未计入管理端统计
- 初始状态：未解决
- 严重度：P2
- 创建时间：2026-10-16 23:43（Asia/Shanghai）
- 模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）
- 确认版本：WORKTREE，基准提交 3ff689706e788264b02839c4e2db9bd8a4644d50；尚未固定为提交。
- 相关未提交文件：qtask_list/admin.py、qtask_list/state.py、qtask_list/worker.py；调用路径位于 qtask_list/queue.py，目前该文件相对基准没有修改。
- 引入提交：未知；当前已定位到本轮 WORKTREE 中管理端取消扫描、仅从 Worker 注册表汇集 processing 的行为变化。不存在可填写的本轮提交 ID。
- 来源：本次会话 Review 的独立 Redis 复现及当前代码核对；[结构化复现结果](../test/20261016-19-WORKTREE/processing-index-review.json)。本次文档维护未重新运行复现，已确认相关代码路径仍在。

## 问题、触发条件与影响

使用当前公开接口 SmartQueue(processing_key=...) 构建消费者，经 enqueue(TaskSpec(...)) 投递 V2 新任务，再调用 pop_claim_no_wait() 领取到专属 processing List。此路径没有经过 Worker._refresh_heartbeat()，不会登记相应的 qtask:workers:<queue> 成员。

QueueAdmin.queue_stats() 从 list_workers() 返回的 processing_key 汇集专属 List，再添加默认 <queue>:processing。list_workers() 现在只读取 Worker 注册表，因此遗漏上述专属 List。

复现时任务记录 location 为 processing，实际 List 长度与 SmartQueue.get_stats()["processing"] 均为 1，但 QueueAdmin.queue_stats()["processing"] 为 0，list_workers() 为空。基准提交的 QueueAdmin 在相同数据上返回 1。这会让 Dashboard/CLI 的执行中、总待处理和活跃队列统计偏低，并遗漏该消费者的待恢复任务入口。

这不是未注册旧队列的兼容需求：队列由当前 enqueue() 登记，任务是本次新建的 V2 任务，使用的也是当前公开消费 API。

## 确认依据与复现

- qtask_list/queue.py:97：SmartQueue 仍公开接受 processing_key。
- qtask_list/queue.py:634：pop_claim_no_wait() 将消息迁入 self.processing 并进入当前领取状态机。
- qtask_list/admin.py:95：队列统计只汇集已发现 Worker 的专属 processing，另加默认 processing。
- qtask_list/admin.py:209：Worker 发现只读取 qtask:workers:<queue>。
- [processing-index-review.json](../test/20261016-19-WORKTREE/processing-index-review.json)记录 WORKTREE 返回 0、基准实现返回 1 的对比。

以下片段中的 r 必须来自 tests/conftest.py 或另一个私有 Redis，不使用共享业务实例：

```python
from qtask_list import QueueAdmin, SmartQueue, TaskSpec

q = SmartQueue(redis_client=r, queue_name="review",
               processing_key="review:processing:custom")
q.enqueue(TaskSpec("work", {"value": 1}))
claim = q.pop_claim_no_wait()
assert claim is not None
assert r.llen(q.processing) == 1
assert q.get_stats()["processing"] == 1
assert QueueAdmin(redis_client=r).queue_stats("review")["processing"] == 0
```

## 临时绕过与完成标准

通过 Worker 类消费并使用其心跳登记路径，可避免该触发条件；这不代表自定义消费者的统计已修复。

修复应完善当前领取路径的 processing 登记或统一 processing 索引，遵守用户“不需要 V1 兼容”的要求，不恢复旧数据扫描。验收覆盖：V2 新任务进入自定义 processing 后管理端计数为 1；ack、fail、recover 后计数正确；消费者退出及故障情况下仍可定位待恢复任务，正常 Worker 和默认 processing 不回归。

关联交接：[docs/HANDOFF.md](../HANDOFF.md)。本次仅记录，尚未实施修复，也未提交或推送。

## 2026-10-17 00:08（Asia/Shanghai）修复启动事件

- 模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 状态：未解决 → 处理中。依据：用户明确要求修复，已开始按关联计划实施。
- 实施版本：WORKTREE，基准提交 `3ff689706e788264b02839c4e2db9bd8a4644d50`；内容尚未固定为提交。
- 关联计划：[PLAN-20261017-001-model-unknown-custom-processing-index.md](../plans/PLAN-20261017-001-model-unknown-custom-processing-index.md)。
- 初始范围：新增队列级自定义 processing-key 登记，统一管理统计、查看和恢复枚举；不恢复全库旧数据发现扫描，不改变公开方法签名。

## 2026-10-17 00:15（Asia/Shanghai）修复验证与解决事件

- 模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 状态：处理中 → 已解决。依据：自定义 processing key 在阻塞及非阻塞领取前写入每队列注册表；管理统计、key 枚举和队列级恢复读取该注册表，ack/fail 结算计数回归正确，队列删除会清理索引。
- 实施版本：WORKTREE，基准提交 `3ff689706e788264b02839c4e2db9bd8a4644d50`；相关修改尚未提交。
- 验证：定向回归 9 passed；全量 Python 250 passed、80 warnings；前端 15 passed；Ruff、mypy（20 个源文件）和 `git diff --check` 通过。测试与环境详情见[测试记录](../test/20261017-00-WORKTREE/自定义-processing-修复测试.md)，实施过程见[计划结果](../plans/PLAN-20261017-001-model-unknown-custom-processing-index-OUTCOME.md)。
- 保留边界：修复前已经存在但从未登记的自定义 processing key 不会扫描回填；调用方需通过领取 API 登记，或按独立维护流程处理已知 key。这符合用户“不需要兼容性”的约束，不影响新领取路径。
- 未提交、未推送。

## 2026-10-17 00:18（Asia/Shanghai）补充复验事件

- 模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 状态保持：已解决。补入管理端 processing 列表实际读取断言后，定向用例 9 passed、132 deselected；全量 Python 250 passed、80 warnings（18.79s）。前端 15 项、Ruff、mypy 和 `git diff --check` 仍通过。
- 依据版本：WORKTREE，基准提交 `3ff689706e788264b02839c4e2db9bd8a4644d50`；仍未提交。
- 更新后的完整记录：[测试记录](../test/20261017-00-WORKTREE/自定义-processing-修复测试.md)和[计划结果](../plans/PLAN-20261017-001-model-unknown-custom-processing-index-OUTCOME.md)。
