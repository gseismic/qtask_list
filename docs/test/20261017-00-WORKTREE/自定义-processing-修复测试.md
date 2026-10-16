# 自定义 processing 修复测试记录

- 测试时间范围：2026-10-17 00:14–00:48（Asia/Shanghai）。
- 测试模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 被测版本：完整工作区基于 `3ff689706e788264b02839c4e2db9bd8a4644d50`；隔离提交候选内容已确认为 commit `054f12c25d4a248a5adf80a3c1c8e18add818458`（分支 `main`，未推送）。
- 测试目的：验证直接使用自定义 `processing_key` 的 SmartQueue 在领取前登记 key 后，管理端统计、枚举、恢复与终态结算正确，并确认既有 Worker 注册表路径不回归。
- 环境：Linux、Python 3.12、Redis 7.1.0。Python 测试由 `tests/conftest.py` 启动临时 Redis Unix socket；未连接共享 Redis `127.0.0.1:6379`。前端测试在 `qtask_list/frontend/` 运行。
- 相关 WORKTREE 文件：`qtask_list/admin.py`、`qtask_list/queue.py`、`qtask_list/state.py`、`qtask_list/worker.py`、`README.md`、`tests/test_core_reliability.py`、`tests/test_dashboard_api.py`、`tests/test_cli.py`、`docs/HANDOFF.md`、`docs/KNOWN-ISSUES.md`、`docs/known-issues/KI-20261016-2343-001-custom-processing-index.md`、`docs/plans/`。同一工作区还包含此前已存在的 Dashboard 前端、静态资源及测试改动；均保留。

## 预期结果

- 自定义 key 在阻塞或非阻塞领取之前写入每队列 processing-key 注册表；索引类型错误时不移动 ready 任务。
- 领取后 QueueAdmin 与 Dashboard 统计 processing 数为 1；ack 或 fail 后归零。
- 消费者退出且无 Worker heartbeat 时，QueueAdmin 能枚举并恢复已登记 key；整个验证不需要 Redis 键空间扫描。
- 删除队列时删除自定义 processing-key 注册表；普通 Worker key 仍从 Worker 注册表发现。

## 执行步骤与结果

| 命令/操作 | 预期 | 实际 |
| --- | --- | --- |
| `uv run --no-project python -m pytest tests/test_core_reliability.py tests/test_dashboard_api.py -k 'custom_processing or processing_registry or delete_queue_removes or recovery_restores_deadline' -q` | 自定义 key 统计、领取登记、ack/fail、恢复、类型冲突、删除及 Dashboard API 用例通过 | 9 passed，132 deselected |
| `uv run --no-project python -m pytest tests/ -q` | 全量 Python 测试通过 | 250 passed，80 warnings，18.79s；完整输出见 [`pytest-full.log`](./pytest-full.log) |
| `uv run --no-project python -m pytest tests/ -q --disable-warnings`（从基准提交加暂存补丁创建的隔离 worktree） | 验证实际提交候选 | 224 passed，80 warnings，19.25s；Ruff、mypy 同一候选副本均通过，输出见 [`pytest-staged-candidate.log`](./pytest-staged-candidate.log) |
| `pnpm test`（工作目录 `qtask_list/frontend/`） | 前端单元测试通过 | 15 passed，0 failed；输出见 [`frontend-test.log`](./frontend-test.log) |
| `uv run --no-project python -m ruff check .` | Ruff 无问题 | All checks passed |
| `uv run --no-project python -m mypy qtask_list` | 类型检查通过 | 20 source files，无问题 |
| `git diff --check` | 无空白错误 | 通过 |

首次全量 Python 测试有 3 个用例失败：它们仅创建 `:processing:<worker>` List 和 heartbeat，没有登记 `qtask:workers:<queue>`。当前发现契约只从该注册表读取 Worker，因此为这些 CLI/Dashboard 测试夹具补上显式 Worker 注册后复跑，全量测试通过；没有恢复扫描兼容。首次在仓库根目录运行 `pnpm test` 因根目录无 `package.json` 未执行，随后在实际前端目录重跑并通过。

## 结果与边界

自定义 processing 统计、枚举和恢复问题在本次 WORKTREE 修复后通过上述回归。全量 Python 运行中的 80 条警告为既有 deprecation warnings。没有运行 `pnpm build`：本次没有修改前端源代码。修复前已存在且从未登记的自定义 processing key 不会通过扫描自动回填；调用方需在新代码下对该 key 调用领取 API 完成登记，或按独立维护流程处理已知 key。此边界符合“不需要兼容性”约束。
