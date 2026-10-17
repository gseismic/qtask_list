# Examples

`qtask_list` 分层示例：从最简生产/消费到生产级模式，按编号递进学习。
所有示例默认连接 `redis://localhost:6379/0`，需要先启动 Redis。

## 学习路径

| 目录 | 主题 | 运行方式 |
|---|---|---|
| `01_basics/` | 最简生产/消费（裸 SmartQueue，理解可靠消费模型） | 2 个终端 |
| `02_worker/` | Worker + handler 注册 + 错误分类与重试 | 2 个终端 |
| `03_retry_dlq/` | 重试退避、DLQ 全流程与 replay | 1 条命令 |
| `04_delay_deadline/` | 延迟执行 vs 业务截止时间 | 1 条命令 |
| `05_idempotency/` | logical_key 身份、两段去重、EnqueueResult | 1 条命令 |
| `06_pipeline/` | 三级跨 namespace 流水线（emissions 级联） | 4 个终端 |
| `07_advanced/` | 确定性调度、动态 fan-out、Reconciler | 各自独立 |
| `08_large_payload/` | 大 payload 自动外存（RemoteStorage） | 3 个终端 |
| `09_admin_ops/` | QueueAdmin 诊断、查任务、重放、恢复 | 3 个终端（复用 02） |

每个目录内的 README.md 有详细的运行步骤与学习点说明，请先阅读。

## 快速开始

```bash
pip install -e .

# 最简体验（01_basics）
python examples/01_basics/consumer.py   # 终端 1
python examples/01_basics/producer.py   # 终端 2
```

## 核心概念对照

| 概念 | 示例 |
|---|---|
| 可靠消费（BRPOPLPUSH + processing + ack） | 01 |
| 错误分类（Retryable / Permanent） | 02 |
| 状态机（delay / retry_wait / dlq / deadline_missed） | 03, 04 |
| 任务身份与幂等（logical_key / dedup_until） | 05, 07 |
| TaskResult.emissions 级联投递 | 06 |
| TaskContext（attempt / trace / 协作停止） | 06, 07 |
| RemoteStorage 透明外存 | 08 |
| QueueAdmin 管理 API | 09 |

## 调试工具

```bash
python -m qtask_list.cli status                          # 所有队列状态
python -m qtask_list.cli watch demo:jobs                 # 实时监控某队列
python -m qtask_list.cli peek demo:jobs --state dlq      # 查看死信
python -m qtask_list.cli history demo:jobs -l 20         # 任务历史
python -m qtask_list.cli web                             # Web 控制台
```
