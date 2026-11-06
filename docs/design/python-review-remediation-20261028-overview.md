# Python 全量 Review 修复：接口与并发设计

- 创建时间：2026-10-28 18:05（Asia/Shanghai）。
- 模型：Codex，环境声明基于 GPT-6，未提供具体模型 ID。
- 分析范围：WORKTREE，基准提交 `4eb01e0907233de19f2a1508babd8ef7f1b7d746`。涉及 `qtask_list/` 的队列、Worker、管理、Dashboard、历史和归档实现以及相关示例、测试；内容尚未固定为提交。
- 依据：`docs/plans/PLAN-20261028-001-model-unknown-python-review-remediation.md` 和本次审查及隔离 Redis 验证。

## 适用场景与接口

| 场景 | 常用入口 | 行为和边界 |
|---|---|---|
| 生产任务并执行 | `SmartQueue.enqueue`、`Worker.run` | 生产者将 `max_attempts` 写入任务实例；Worker 不覆盖已有预算。旧消息迁移使用 Worker 内部队列的配置。 |
| 多 Worker 消费 | `Worker(..., worker_id=...)` | 同队列实例身份不可重复；冲突抛 `WorkerIdentityConflictError`。不同队列可复用同一 ID。 |
| 崩溃后恢复 | `Worker.run`、`QueueAdmin.recover` | 自动恢复失联的已登记 Worker；不自动接管没有心跳契约的自定义消费者。 |
| 直接消费者停止后恢复 | `SmartQueue.recover`，或 `QueueAdmin.recover(..., include_active=True)` | 前者处理当前 key；后者可同时接管活跃 Worker，调用者须先确认所有受影响消费者停止。 |
| 清理自定义消费者的消息 | `force_unmanaged=True` | 队列清理、单任务删除和队列删除须明确确认该消费者已停止；活跃 Worker 的保护仍生效。 |
| 本机或远程 Dashboard | `qtask web`、`start_dashboard` | 本机可无认证；远程监听需要认证，或显式允许无认证。直接使用 uvicorn 的部署方负责配置认证。 |
| 跨域 Web 集成 | `QTASK_DASHBOARD_ALLOWED_ORIGINS` | 默认允许同源写入；明确配置可信来源后允许其跨域请求。无 `Origin` 的服务端客户端继续可用。 |
| 注入 Redis 与归档 | `ArchiveManager(..., redis_client=...)` | Worker 和归档使用同一连接；同目录归档者串行，Redis 记录变化时不删除该记录。 |

## 设计取舍和生命周期

1. Worker 保留带 TTL 的时间型 heartbeat，并另外持有带随机实例令牌的身份租约；登记、续租与释放原子比较令牌。与仅检测 heartbeat 相比，这可以区分过期的旧进程和同 ID 的新进程，也保持现有监控对 heartbeat 的读取方式。领取开始、旧消息迁移、无效消息处理及自身遗弃消息恢复检查令牌；失权后已阻塞领取但尚未开始的消息退回 ready。租约不是外部事务锁，已运行的 handler 仍可能与重试重叠。
2. 没有 heartbeat 的直接消费者既可能存活，也可能崩溃；默认跳过自定义及默认 processing。管理调用方明确确认后可恢复或清理。相比按 key 名推测 Worker，该行为避免抢走仍在执行的任务，但需要既有自动化调整参数和停机流程。
3. 区分 Worker 登记与自定义 processing 登记，登记时防止队列状态键或其他消费者占用同一自定义 key。既有冲突不会自动改名或清理，须在维护窗口处理。
4. Origin 检查覆盖跨域简单写请求，CORS 允许列表覆盖浏览器预检；仅关闭 CORS 不能阻止无需预检的写请求。没有 Origin 的调用继续工作，因此认证、网络边界和可信反向代理仍由部署方配置。
5. 归档先持久化 SQLite，再比较 Redis 原始快照并条件删除。共享目录内用 SQLite 锁串行化归档，避免较旧快照覆盖较新快照。SQLite 与 Redis 不具备跨库原子性；异常时可能同时存在旧归档和更新后的 Redis 记录，下次归档可收敛。锁要求所有归档者使用同一个目录和前缀；无共享文件系统的部署不能依赖此锁。
6. `retry_wait` 是 delay 的子集，管理重入队检查消息的延迟原因并迁移到 ready；不将其他定时任务误判为重试。批量兼容接口顺序提交，并在写入前拒绝负的相对时间参数。
7. 自定义外存适配器的 `save_bytes` 旧签名在调用前识别，适配器内部 `TypeError` 不触发第二次上传；无法检查签名的对象按带 `retain_until` 的协议调用。读取和保存中的未分类异常转换为不含原文的外存错误。
8. RemoteStorage 在共享数据目录使用 SQLite 锁串行化保留期写入和 TTL 删除；元数据经临时文件原子替换，不可解析时跳过清理。上传创建文件及设置初始保留期、检查文件存在并延长保留期，均在相同锁内。该锁依赖共享文件系统支持 SQLite 文件锁；与 Redis GC 的业务生命周期仍分别处理。
9. RemoteStorage 的无 token 本机服务也可能收到浏览器跨站 multipart 上传；有 Origin 的写请求只接受同源，无 Origin 的 SDK 请求维持现有契约。反向代理需正确转发外部协议和 Host。

## 兼容性与错误

- 安全默认值会拒绝过去允许的无认证远程启动，以及未确认的自定义 processing 清理。迁移方式是配置密码、可信跨域来源或在消费者停止后显式指定相应参数。
- `qtask task replay -f` 保留给 payload 文件；跳过确认使用 `-y` 或 `--force`。原来冲突的 `-f` 不再兼作确认。
- 业务处理维持至少一次交付，handler 与外部写入应使用幂等键；不承诺旧 handler 在租约到期后被强制终止。
- Dashboard 返回外存错误类别和库内固定错误码，不返回异常原文或第三方自定义错误码；payload 读取失败的任务历史同样只保存安全错误说明。运维应结合受控日志定位失败。
- Redis 或 SQLite 错误仍向调用者报告；归档目录需可写，SQLite 锁等待默认最多 30 秒。批量提交可部分成功，`BatchEnqueueError` 提供已完成前缀。

## 设计复核

复核覆盖身份首次占用、重复启动、租约过期后旧实例清理和阻塞领取、启动期间恢复、自定义 processing 登记与删除、浏览器有无 Origin、并发重放与归档，以及已有任务预算。关键约束是令牌检查必须与会改变 Redis 状态的操作处于同一原子脚本；跨 Redis 与 SQLite 的变化不能声称是单事务。仍需通过隔离 Redis 的交错测试和全量测试验证实现。
