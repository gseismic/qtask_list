# Python 全量审查修复测试记录

- 测试开始：2026-10-28 17:38（Asia/Shanghai）。
- 执行模型：Codex，环境声明基于 GPT-6，具体模型 ID 未提供。
- 测试范围：WORKTREE，基准提交 `4eb01e0907233de19f2a1508babd8ef7f1b7d746`；本计划、计划索引、`tests/test_python_review_remediation.py` 和本测试记录尚未提交，后续修改见结果文件。
- 环境：`uv run --no-project` 使用当前 Python 环境；pytest 的 `tests/conftest.py` 启动仅通过临时 Unix socket 访问的独立 Redis，并使用临时归档目录。
- 目的：先复核 Review 中的可确定缺陷，再验证修复；不连接本机业务 Redis，也不运行原有会清库的手工脚本。

## 修复前基线

- 命令：`uv run --no-project python -m pytest tests/test_python_review_remediation.py -q --disable-warnings --tb=short`。
- 预期：理想行为检查指出尚未修复的缺陷；生产者任务预算保留测试通过。
- 实际：10 失败、1 通过。失败覆盖无心跳消费者被自动恢复、重名 Worker 未拒绝、无认证远程启动、跨域写入、外存异常泄漏、retry_wait 重入队、三个批量负时间参数及归档与重放竞态；预算随生产者任务保存的测试通过。
- 输出：[python-review-baseline.txt](./python-review-baseline.txt)。失败是修复前复现结果，不是修复验证通过。

## 首轮修复和手工验证

- 工作区经过第一轮修复后，相关队列、可靠性、注册表和新增审查回归共 117 项通过；该结果不代表后续修改的最终验证。
- 手工脚本均启动独立 Unix socket Redis：`uv run --no-project python tests/manual/e2e_plan014.py` 和 `uv run --no-project python tests/manual/verify_review_claims.py` 均退出成功。日志分别为 [manual-e2e.txt](./manual-e2e.txt) 和 [manual-state-check.txt](./manual-state-check.txt)。端到端日志中的两次模拟连接错误用于验证重试，随后第三次成功。
- 本时段的相关未提交文件：`qtask_list/__init__.py`、`qtask_list/admin.py`、`qtask_list/archiver.py`、`qtask_list/cli/__main__.py`、`qtask_list/dashboard/main.py`、`qtask_list/errors.py`、`qtask_list/queue.py`、`qtask_list/security.py`、`qtask_list/state.py`、`qtask_list/worker.py`、`tests/test_core_reliability.py`、`tests/test_queue.py`、未跟踪的 `tests/test_python_review_remediation.py` 与 `tests/manual/_isolated_redis.py`、修改的手工脚本以及本次计划和测试记录。
- 后续完整工作区和最终复测见 `docs/test/20261028-18-WORKTREE/Python审查修复测试.md`。
