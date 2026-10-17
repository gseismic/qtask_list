# 交接文档（HANDOFF）

- 更新时间：2026-10-17 00:18（Asia/Shanghai）。
- 文档模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 当前版本：WORKTREE；分支 main，基准提交 3ff689706e788264b02839c4e2db9bd8a4644d50。下述实现含大量未提交、未跟踪文件，不能把基准提交当作这些修改的固定版本。
- 交接目的：记录 Dashboard 修复、被中断的 Review，以及随后完成的自定义 processing 修复。
- 本次修复自定义 processing 登记与管理端统计/恢复，新增回归并完成全量验证；没有提交或推送。此前工作区中的用户和其他任务改动均保留。

## 1. 项目与用户约束

qtask_list 是基于 Redis 的任务队列库，当前版本 0.2.0；后端使用 Python/FastAPI，管理端为 qtask_list/frontend/ 下的 React、TypeScript、Vite 项目，使用 pnpm。运行时 SPA 位于 qtask_list/dashboard/static/spa/。

开始任务先检查 GOAL.md；当前不存在，不要擅自创建。AGENTS.md 有用户原有未提交修改，须保留。提交和推送分别需要明确授权，本次没有这两项授权。

用户已明确表示不需要兼容性。对于队列和 Worker 的自动发现，已采用注册表作为唯一来源，删除 V1 发现扫描及缓存。旧交接中“必须保留 V1 自动发现”的要求已被取代。后续修复应完善当前数据的登记，不恢复旧扫描。其他模块的旧消息读取、重试等兼容分支并未在这轮全面删除。

## 2. 已完成工作

### 注册表发现与 Worker 生命周期

见[PLAN-20261016-002 结果](./plans/PLAN-20261016-002-model-unknown-registry-discovery-OUTCOME.md)。

- qtask_list/admin.py 的 queue_names() 直接读取 qtask:queues，list_workers() 读取各队列的 qtask:workers:<queue>。
- qtask_list/state.py 和 qtask_list/worker.py 实现原子登记队列、Worker 和 TTL 心跳，写入前检查键类型及 namespace 冲突；空闲 Worker 也登记队列。
- Worker 退出但仍有 processing 任务时保留注册项，供失联诊断和恢复；失效注册项清理原子复核心跳及 processing，保护并发续租。
- 已登记的空逻辑队列保留；外部移除注册项会在下一次查询反映，运行中的 Worker 会在下一次心跳重新登记。
- README.md、tests/test_registry_discovery.py 及 API/CLI 样本已同步。

### 搜索误发详情请求

见[PLAN-20261016-001 结果](./plans/PLAN-20261016-001-model-unknown-dashboard-handoff-fixes-OUTCOME.md)。该计划中早期保留兼容发现的部分已由 PLAN-002 替代，搜索修复继续有效。

- search_tasks_page() 的第一页返回服务端确认的 exact_task_id；普通关键词不再靠正则猜测 ID 并预请求详情。
- qtask_list/frontend/src/pages/TasksPage.tsx、components/TaskTable.tsx、useTaskPage.ts、types.ts 使用当前列表查询的确认结果打开详情，防止旧响应和重复轮询重新打开抽屉。

### 总览 KPI 点击反馈

用户截图反馈的“4 张卡点击没反应”已在后续修改中处理，见[PLAN-20261016-003 结果](./plans/PLAN-20261016-003-model-unknown-overview-kpi-interactions-OUTCOME.md)与[交互测试记录](./test/20261016-19-WORKTREE/总览-KPI交互测试.md)。本次交接已核对当前代码与这些记录，未重复执行浏览器测试。

- “活跃队列”和“总待处理”跳转至 /queues?filter=active。
- “异常队列”筛选异常、展开匹配分组并滚动定位矩阵。
- 可点击卡片增加 hover/focus 操作提示、键盘焦点和可访问名称。
- “合计观测速率”保持只读，不应作为按钮验收。
- 相关代码为 qtask_list/frontend/src/pages/OverviewPage.tsx、qtask_list/frontend/src/components/ui.tsx、qtask_list/frontend/src/styles.css。
- 目前 SPA index.html 引用 index-5vhg9Z4O.js 和 index-B0zgLNSh.css；更早的 index-BHQL5wkx.js 已被后续构建替代。旧 JS/CSS 的删除属于构建产物更新。

总览修复记录指出：目标队列页仍沿用其保存的 namespace/名称筛选，可能进一步缩小下钻列表。复测时应核对这些筛选；是否改变该产品行为尚未定案。

## 3. 已知问题状态

[KI-20261016-2343-001：自定义 processing 消费任务未计入管理端统计](./known-issues/KI-20261016-2343-001-custom-processing-index.md) 已在 WORKTREE 修复并验证，严重度 P2；详情已追加解决事件并归档。

修复前的私有 Redis 复现使用 SmartQueue(processing_key="review:processing:custom")，enqueue(TaskSpec(...)) 投递后再以 pop_claim_no_wait() 领取一个 V2 新任务：

| 观测 | 结果 |
| --- | --- |
| 任务记录 location | processing |
| 实际专属 List 长度 | 1 |
| SmartQueue.get_stats() 的 processing | 1 |
| 当前 QueueAdmin.queue_stats() 的 processing | 0 |
| 当前 QueueAdmin.list_workers() | 空 |
| 同数据下基准提交的 QueueAdmin processing | 1 |

[结构化复现结果](./test/20261016-19-WORKTREE/processing-index-review.json)保留修复前证据。当前 `SmartQueue` 在阻塞和非阻塞领取前将自定义 processing key 登记到 `qtask:processing:<queue>`；管理统计、查看和恢复从该注册表、Worker 注册表及默认 key 枚举。队列删除时清除索引，不扫描 Redis 键空间，也不为自定义消费者伪造 Worker heartbeat。

定向回归覆盖领取前登记、管理端与 Dashboard 统计、ack/fail、无 heartbeat 的退出恢复、索引类型冲突和队列删除。修复前已经存在但从未登记的自定义 processing key 不会自动回填；调用方可在新代码下对该 key 调用领取 API 登记，或按独立维护流程处理。这是按用户“不需要兼容性”约束保留的边界。完整结果见[计划结果](./plans/PLAN-20261017-001-model-unknown-custom-processing-index-OUTCOME.md)及[测试记录](./test/20261017-00-WORKTREE/自定义-processing-修复测试.md)。

早先两项问题 KI-20261016-1218-001、KI-20261016-1218-002 已解决并归档；当前状态以[已知问题索引](./KNOWN-ISSUES.md)及各详情的最后事件为准。

## 4. 验证依据与覆盖空白

| 阶段 | 已有结果 | 覆盖边界与证据 |
| --- | --- | --- |
| 注册表发现修复 | Python 全量 243 passed；前端 15 passed；浏览器 18 项；ruff/mypy 通过 | [18 时测试记录](./test/20261016-18-WORKTREE/测试记录.md)。此阶段早于总览 KPI 后续补丁。 |
| 本次会话的 Review | Python 定向 101 passed；前端纯逻辑 15 passed；独立 Redis 确认自定义 processing 遗漏 | 指令为 pytest tests/test_registry_discovery.py tests/test_dashboard_api.py tests/test_cli.py -q，以及 pnpm test。没有新增或修改产品测试文件。 |
| 总览 KPI 后续补丁 | pnpm build 及浏览器下钻、异常展开定位、hover/focus、键盘和告警定位检查通过 | [19 时交互测试记录](./test/20261016-19-WORKTREE/总览-KPI交互测试.md)记载至 19:55；该阶段没有重跑前端单元套件。 |
| 本次交接维护 | 核对当前代码、状态、报告、构建资源及正在运行的服务 | 未重跑产品测试。此前 243 项通过不表示已覆盖本次 Review 发现的缺陷。 |
| 自定义 processing 修复 | Python 250 passed；前端 15 passed；ruff、mypy 和 diff check 通过 | [00 时测试记录](./test/20261017-00-WORKTREE/自定义-processing-修复测试.md)。三个 Worker 测试夹具补登记注册表后，全量测试通过。 |

Review 的最初 KPI 复现证据 overview-kpi-review.txt、overview-reviewed.yml 属于修复前状态；当前 KPI 以 PLAN-003 和其后续测试快照为准，不能继续把旧交互当作未修复问题。

修复前 KPI 证据与自定义 processing 复现仍保留在历史测试目录；判断当前行为时分别以 PLAN-003 和本次修复测试记录为准。

Python 测试沿用 tests/conftest.py 在收集前创建的私有 Unix socket Redis；建议继续使用 uv run --no-project python -m pytest。前端使用 pnpm test 和 pnpm build。所有新增测试产物按 AGENTS.md 放入对应时段的 WORKTREE 测试目录，保留已有记录。

## 5. 工作区与运行环境

所有上述代码和文档仍处于 WORKTREE。除源代码修改外，还有未跟踪的 docs/plans/、三轮测试产物、已知问题归档、tests/test_registry_discovery.py、tests/manual/handoff_dashboard_server.py、tests/manual/handoff_tasks_search.js，以及新 SPA 资源。后续获得提交授权时，先检查完整 git status 和差异，区分用户原有 AGENTS.md 改动及各轮任务，不直接暂存整个目录。

2026-10-16 23:43 核对时，以下服务仍在运行并可访问：

- Dashboard：http://127.0.0.1:56643；/api/health 返回 ok，首页引用上述最新 JS/CSS。
- 启动方式：uv run --no-project python tests/manual/handoff_dashboard_server.py /tmp/qtask-dashboard-live.ohRJC0。
- 对应 Python 进程 PID 605371，父进程 PID 605368，私有 Redis 进程 PID 605372；PID 和临时路径仅是当前观测，后续先重新核对。
- 私有 Redis socket：/tmp/qtask-handoff-browser-f34z37hu/redis.sock。运行信息见[浏览器运行信息](./test/20261016-19-WORKTREE/browser-runtime.json)及 /tmp/qtask-dashboard-live.ohRJC0/browser-runtime.json。

该服务由后续 KPI 工作保留供用户复测，本次没有停止、重启或清理。之前 13/18 时测试环境“已清理”的记录只针对各自临时实例，不能据此停止当前服务。共享 Redis 127.0.0.1:6379 进程也存在，本次未访问其业务数据。

## 6. 建议接续步骤

1. 自定义 processing 问题已完成并归档；如未来需要处理修复前未登记的历史 key，应按当时明确授权的维护范围单独处置，不恢复自动键扫描。
2. 如继续复核总览，使用最新 SPA，检查目标页保存的筛选条件，并区分可点击的 3 张 KPI 与只读速率卡。

Review 在回复完整结论前被中断；本次完成的是其中单独记录的 KI 修复，不代表整个工作区已完成 Review。用户后续开发、提交和推送的授权应依实际请求判断。
