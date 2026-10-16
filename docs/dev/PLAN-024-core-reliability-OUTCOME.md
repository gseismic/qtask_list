# PLAN-024 执行结果：核心可靠性修复与循环复查

- 更新时间：2026-10-16 08:25（Asia/Shanghai）
- 执行模型：Codex（环境未提供具体模型 ID）
- 计划：`docs/dev/PLAN-024-core-reliability.md`
- 设计：`docs/design/core-reliability-20261016-remediation.md`
- 本文件生成时实现状态：WORKTREE，基准提交 `20b4b594c107d2390c4b157d5ff88722458bc87f`；本文件与实现同次提交。
- 范围：非 UI 核心、管理 SDK、README、测试；未修改前端、页面或样式。

## 原 11 项审查问题的结果

| 问题 | 修复结果 | 核心回归（`tests/test_core_reliability.py`） |
| --- | --- | --- |
| Lua 改写业务 JSON | Python 构造保真模板，Lua 只填 attempt/available_at；兼容现存 V2 信封 | `test_payload_is_exact_through_claim_retry_and_replay`、`test_concurrency_deferral_preserves_payload` |
| Redis 故障后滞留 processing | 结算重试保留本地 claim/租约；原子恢复未登记消息，核验 claim token 后恢复未准入预算 | `test_worker_recovers_interrupted_begin_without_touching_active_claim`、`test_worker_retries_settlement_without_reexecuting_handler`、`test_committed_begin_response_loss_restores_unadmitted_budget` |
| 队列键冲突、部分写入误报成功 | 注册与实际命名冲突核验位于同一投递脚本；写入前检查键/计数类型；仅以最终提交标记确认响应丢失 | `test_queue_namespace_overlap_is_rejected_in_both_directions`、`test_enqueue_type_error_has_no_partial_identity`、`test_invalid_metric_counter_does_not_partially_enqueue` |
| 短租约并发失效 | 续期取 heartbeat 与并发 lease 周期的较小值，拒绝无效或不足 1ms 的租约配置 | `test_short_lease_is_renewed_while_handler_runs`、`test_invalid_timing_configuration_fails_before_writes` |
| String 历史无法迁移/清理 | CAS 保留 TTL、嵌套 JSON 和终态；旧 DLQ 记录受真实 operational message 保护 | `test_legacy_string_history_can_execute_and_clean`、`test_old_terminal_string_without_operational_flag_can_clear` |
| Redis 故障终止维护线程 | 失败计数写入独立保护，故障恢复后维护循环继续 | `test_maintenance_survives_redis_and_metrics_outage` |
| 字典/列表结果无法归档 | 结构化结果显式编码为 SQLite 可绑定 JSON，布尔值保持 JSON 类型 | `test_structured_results_are_archived_as_json` |
| 抽样安全修复误改 DLQ | 未扫描到的消息标为未知；修改终态标记前额外检查真实容器 | `test_sampled_consistency_does_not_unprotect_dlq` |
| 新旧 dedup 键混用 | 新 owner 位于独立全局前缀；按类型、原身份迁移旧 owner，并保留终态 TTL | `test_hash_and_plain_logical_keys_are_independent`、`test_legacy_raw_key_cannot_alias_versioned_owner`、`test_legacy_retained_owner_keeps_terminal_state_and_ttl` |
| supersede 大整数比较失真 | 按有符号十进制文本精确比较，不经 Lua double | `test_supersede_compares_integer_versions_exactly` |
| V1 外存故障无限重试 | 下载前原子迁移到 V2 descriptor，复用 attempt/deadline/DLQ/replay；只对旧格式剥离 `_retry` | `test_legacy_external_failure_has_budget_and_replay`、`test_legacy_payload_migration_resolves_action_without_leaking_retry`、`test_legacy_expired_external_task_never_downloads` |

## 三轮复查与修正

1. 首轮修复后发现一律禁止状态段会拒绝已有合法 `testns:dedup` 队列。改为核验真实父子冲突，不修改旧测试名称。首轮验证后 **169 passed**。
2. 复查 begin/ack/replay 响应丢失、历史文本类型、计数值错误、保留期 owner、管理删除与恢复。增加 claim token、预算恢复条件、独立 owner 前缀、恢复索引及心跳原子核验。第二轮验证 **188 passed**。
3. 复查外存读取失败后的预算、时钟偏差、旧无效 action、时间配置、动态 GC 键及历史格式转换竞争。外存尝试不误退款，默认任务按 Redis 时间进入 ready；History/Admin/Archive 改为原子快照读取。最终全量 **206 passed**。

最终回归保留原 136 项并新增 70 项参数化/场景用例。测试覆盖真实 Redis Lua 和线程执行，并注入领取、结算、维护、格式转换与响应丢失故障。最后一轮复查未发现本次范围内新的待修复问题。

## 验证

- 全量 pytest：**206 passed，80 warnings**；无失败用例。
- `ruff check .`：通过。
- `mypy qtask_list`：20 个 Python 模块通过。
- 50 个 Python 源文件（核心、examples、tests）编译检查：通过。
- `git diff --check`：通过。
- Python 检查通过 `uv run --no-project` 使用已安装开发依赖的 Python 环境执行。
- `tests/conftest.py` 在收集前启动私有 Unix socket Redis；连接、归档、外存及旧测试相对文件目录均隔离到临时目录，结束后清理，不触及既有 Redis 数据。

## 兼容与部署约束

- 无前端改动，保留现有 V2 信封格式；修复前已损坏的业务数值不能凭空恢复。
- 更新身份键格式时应停止旧生产者与消费者，再统一启动新版。旧 owner 会按实际访问身份迁移；旧版本不识别新 owner 前缀。
- 不自动重命名、删除已有冲突队列；内部前缀、glob 字符、空段和实际状态路径冲突会明确报错。
- at-least-once、外部副作用幂等、handler 自带 I/O 超时的原有边界保持。没有承诺线程强杀或外部系统 exactly-once。
- 实现、设计、计划及本结果按仓库约定同次提交，提交消息列出路径和变更；提交后立即推送。
