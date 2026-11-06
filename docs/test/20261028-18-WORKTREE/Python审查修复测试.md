# Python 全量审查修复测试记录：后续复核

- 测试开始：2026-10-28 18:00（Asia/Shanghai）；最后更新：2026-10-28 18:21。
- 执行模型：Codex，环境声明基于 GPT-6，未提供具体模型 ID。
- 范围：WORKTREE；基准提交 `4eb01e0907233de19f2a1508babd8ef7f1b7d746`，所有测试内容尚未提交。
- 环境：`uv run --no-project` 使用 Python 3.12；pytest 由 `tests/conftest.py` 创建仅监听 Unix socket 的私有 Redis 和临时文件目录，手工脚本也各自创建私有实例。未连接业务 Redis 或外部存储。
- 目的：复现及回归 Worker 身份竞态、消费者保护、Dashboard/外存跨来源写入、外存保留和异常脱敏、归档重放竞态、管理状态映射与示例行为。

## 工作区文件

- 源码：`qtask_list/__init__.py`、`qtask_list/admin.py`、`qtask_list/archiver.py`、`qtask_list/cli/__main__.py`、`qtask_list/dashboard/main.py`、`qtask_list/envelope.py`、`qtask_list/errors.py`、`qtask_list/history.py`、`qtask_list/queue.py`、`qtask_list/remote_storage/server.py`、`qtask_list/security.py`、`qtask_list/state.py`、`qtask_list/worker.py`。
- 示例与说明：`README.md`、`examples/04_delay_deadline/demo.py`、`examples/06_pipeline/README.md`、`examples/06_pipeline/finance/calculate_worker.py`、`examples/06_pipeline/stockev/fetch_worker.py`、`examples/07_advanced/README.md`、`examples/07_advanced/news_discover.py`、`examples/07_advanced/reconciler.py`、`examples/07_advanced/scheduler.py`、`examples/08_large_payload/common.py`。
- 测试：`tests/test_core_reliability.py`、`tests/test_queue.py`、`tests/test_python_review_remediation.py`、`tests/manual/dashboard_review_seed.py`、`tests/manual/e2e_plan014.py`、`tests/manual/verify_review_claims.py`、`tests/manual/_isolated_redis.py`。其中新增的审查回归和独立 Redis helper 为未跟踪文件。
- 任务文档：`docs/design/python-review-remediation-20261028-overview.md`、`docs/plans/PLAN-20261028-001-model-unknown-python-review-remediation.md`、`docs/plans/INDEX.md`、对应结果文档、`docs/known-issues/KI-20261016-2343-001-custom-processing-index.md` 以及 `docs/test/20261028-17-WORKTREE/` 和本目录。新增文件均未跟踪。

## 修复前复现

- 外存元数据损坏及保留期延长与清理交错：执行 `uv run --no-project python -m pytest tests/test_python_review_remediation.py -k 'expired_cleanup_preserves_unreadable_metadata or retention_extension_cannot_succeed' -q --disable-warnings --tb=short`，两项均失败，分别观察到文件被删除，以及延长成功后文件仍被删除。日志：[storage-retention-baseline.txt](./storage-retention-baseline.txt)。
- 本机无 token 外存跨来源 multipart 上传：执行 `uv run --no-project python -m pytest tests/test_python_review_remediation.py -k storage_rejects_cross_origin_multipart_upload -q --disable-warnings --tb=short`，预期 403，实际为 200 并创建文件。日志：[storage-origin-baseline.txt](./storage-origin-baseline.txt)。
- 一次新增测试的早期执行出现 3 个断言错误：未知 POST 路由实际为 405，测试队列名称本身包含 `secret`。修正测试的预期和名称后复测通过，记录保留在 [targeted-latest.txt](./targeted-latest.txt)。

## 最终验证

| 操作 | 预期 | 实际与证据 |
|---|---|---|
| `uv run --no-project python -m pytest tests/ -q --disable-warnings --tb=short` | 全量 Python 回归通过 | **343 项通过**，83 条警告；[pytest-full-complete.txt](./pytest-full-complete.txt)。 |
| `uv run --no-project ruff check qtask_list examples tests` | 全部源码、示例和测试检查通过 | 通过；[ruff-all-python-final.txt](./ruff-all-python-final.txt)。 |
| `uv run --no-project python -m mypy qtask_list` | 运行库源码类型检查通过 | 20 个源文件通过；[mypy-all-final.txt](./mypy-all-final.txt)。 |
| `uv run --no-project python tests/manual/e2e_plan014.py` | 在独立 Redis 中验证去重、两次失败后重试、截止和下游任务 | 通过；日志中的两次异常是有意模拟的故障，[manual-e2e.txt](./manual-e2e.txt)。 |
| `uv run --no-project python tests/manual/verify_review_claims.py` | live 历史保留、预算、重放和非法输入通过 | 通过；[manual-state-check.txt](./manual-state-check.txt)。 |
| `uv run --no-project python -m pytest tests/test_python_review_remediation.py -q --tb=short` | 查看新增回归及警告 | 补充最后四项回归前执行：45 项通过，5 条弃用警告，包含 FastAPI `on_event` 和兼容接口 `push_batch`；[warnings-review.txt](./warnings-review.txt)。 |

外存新增覆盖损坏/合法元数据、共享目录跨进程锁、元数据替换失败、并发延长与 TTL 删除、同源/跨来源上传和 GC 原有路径。Worker 新增覆盖重名启动、所有权到期、阻塞领取返回、旧消息迁移、无效消息、启动和遗弃消息恢复。归档覆盖重放交错以及两个归档者顺序。完整断言见 `tests/test_python_review_remediation.py`。

测试未覆盖真实浏览器、真实外部存储网络或不支持 SQLite 文件锁的共享文件系统；这些边界不属于本次隔离回归的通过结论。
