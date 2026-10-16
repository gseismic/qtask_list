# 交接文档（HANDOFF）

- 更新时间：2026-10-16 12:23（Asia/Shanghai）
- 文档模型：Codex / model-unknown（环境声明基于 GPT-6，未公开具体模型 ID）
- 交接依据代码基线：2722fbcfd10f5784fa19b669bba1f5fe21a69535（main，当时与 origin/main 同步）
- 最近浏览器测试基线：8cea280c9007d4e924d5705632c6685a80d53da1
- 交接目的：将 Dashboard 浏览器测试确认的两个问题交给后续修复者
- 工作树保护：AGENTS.md 当前有用户未提交修改；本次未修改、未暂存该文件

## 1. 项目背景

qtask_list 是基于 Redis 的分布式任务队列库，当前版本为 0.2.0。队列提供者、Worker 和管理后台共享 Redis 中的队列与任务状态；项目保留 V1 队列/任务的兼容发现和读取路径。

管理后台由 FastAPI 提供 API 和 SPA 托管，前端源码位于 qtask_list/frontend/，使用 React、TypeScript、Vite 和 pnpm；运行时静态资源位于 qtask_list/dashboard/static/spa/。队列发现及管理逻辑集中在 qtask_list/admin.py，页面 API 路由位于 qtask_list/dashboard/main.py。

项目目标、开发和提交规则以仓库根目录 AGENTS.md 为准。开始修复前应读取该文件；它在当前工作树中含有尚未提交的用户修改。

## 2. 当前状态

- 最近一次完整浏览器回归已完成，覆盖总览、队列、队列详情、任务列表和详情、Worker、告警、教程及可见条件操作。
- 测试结束后清理了本轮临时数据；测试记录称 Dashboard 队列列表及本轮 Redis 注册表、历史、任务、Worker 和队列状态键均为空。
- 本轮没有修改业务代码。当前尚未解决的问题有两项，详情及状态见[已知问题索引](./KNOWN-ISSUES.md)。
- 测试报告、页面快照和截图位于[测试报告目录](./test/20261016-10-8cea280/)。主页面空状态截图为[主页面.png](./test/20261016-10-8cea280/主页面.png)。

## 3. 待修复问题

建议优先处理 P2，再处理 P3。下面的复现都应使用隔离 Redis 或专用测试前缀，不要对用户队列执行清理。

### P2：外部清空 Redis 后 Dashboard 仍显示旧队列

问题详情：[KI-20261016-1218-001](./known-issues/KI-20261016-1218-001-queue-discovery-cache.md)。

代码位置：[QueueAdmin.queue_names](../qtask_list/admin.py)，约第 91–121 行。旧版队列发现结果缓存在进程内；缓存未过期时，queue_names 会把 Redis 当前 qtask:queues 注册表和 _legacy_queue_cache 合并返回。若其他进程在 Dashboard 最近一次扫描后删除旧队列、历史和任务数据，缓存仍可能让 GET /api/queues 返回不存在的队列。本轮观察到注册表已空但界面/API 显示 7 个空队列，重启 Dashboard 后显示恢复。复现截图见[缓存异常复现.png](./test/20261016-10-8cea280/缓存异常复现.png)。

注意：Dashboard 自身的删除路径在 qtask_list/admin.py 的 delete_queue 中会从 _legacy_queue_cache 移除对应队列（约第 1194 行）。已确认问题针对外部 Redis 清理，没有经过这条失效路径。修复应同时保留旧版无注册表队列的发现兼容性，避免简单关闭 legacy 扫描造成旧队列漏报。

建议复现步骤：

1. 在隔离 Redis 中创建一个只通过旧版结构可发现的测试队列，并让同一个 QueueAdmin 实例调用 queue_names，确认它已填充 legacy 缓存。
2. 使用独立 Redis 客户端删除该队列相关的状态键、任务历史及任务记录；不要调用 Dashboard 的 delete_queue。
3. 仍使用原 Dashboard/QueueAdmin 进程请求 GET /api/queues。
4. 观察同一进程是否仍返回已删除队列；当前问题会在旧发现缓存的一小时刷新窗口内复现。

验收建议：

- 外部删除后，现存 Dashboard 进程能在明确且可接受的刷新窗口内停止返回已不存在的队列，不依赖重启或等待整小时缓存自然过期。
- 仍能发现真实存在、没有 qtask:queues 注册项的 V1/旧版队列。
- Dashboard 自身删除队列的行为和既有 legacy 兼容性不回归；为外部删除场景增加同进程回归测试。

### P3：普通关键词搜索误触发任务详情请求

问题详情：[KI-20261016-1218-002](./known-issues/KI-20261016-1218-002-task-search-id-heuristic.md)。

代码位置：[TasksPage.tsx](../qtask_list/frontend/src/pages/TasksPage.tsx)，约第 38–45 行。输入框用于按 task_id、action、payload 搜索；同一字符串若符合“至少 8 位且仅含字母、数字、下划线或连字符”的正则，useEffect 就会调用 api.task(id)。因此普通搜索词也会被当作 task_id 请求详情。本轮搜索 browser_test_dlq 时记录到两次 404；列表关键词搜索本身仍有结果，详情请求错误被 catch 忽略。

新任务 ID 在 qtask_list/queue.py 中由 uuid.uuid4() 生成，但修复前需确认旧数据或调用方是否允许其他 ID 格式，避免只按新 ID 格式识别而破坏兼容。

建议复现步骤：

1. 启动 Dashboard 并进入任务列表。
2. 搜索 browser_test_dlq 或其他至少 8 位、仅含允许字符的普通关键词。
3. 检查浏览器网络请求：当前会额外请求 /api/task/{keyword} 并得到 404；列表搜索仍正常。
4. 使用一条真实 task_id 搜索，核对当前自动打开详情的行为是否需要保留。

验收建议：

- 普通 action/payload 关键词可以正常筛选，且不会触发无意义的详情请求或 404。
- 有效 task_id 的既有查找/打开详情体验仍符合产品约定；无效 ID 不产生无提示的额外请求。
- 增加覆盖普通关键词与有效 task_id 两种情况的前端回归测试。

## 4. 建议验证

Python Dashboard API 测试：

~~~bash
python -m pytest tests/test_dashboard_api.py -q
~~~

前端测试和构建：

~~~bash
cd qtask_list/frontend
pnpm test
pnpm build
~~~

修复完成后再运行完整 Python 测试：

~~~bash
python -m pytest tests/ -q
~~~

当前 tests/test_dashboard_api.py 的 Redis fixture 默认连接 redis://localhost:6379/0，清理范围以 qtask_dash_test 前缀为主；管理后台队列发现会扫描更广的 Redis 键空间。请优先使用隔离 Redis 实例或确认测试数据完全隔离后再运行，避免影响共享队列。

## 5. 交接资料

- [浏览器测试报告](./test/20261016-10-8cea280/测试报告.md)：环境、范围、逐页预期/实际结果、未执行项和最终清理结果。
- [缓存异常复现截图](./test/20261016-10-8cea280/缓存异常复现.png)：外部清理后 Dashboard 显示缓存队列。
- [最终主页面截图](./test/20261016-10-8cea280/主页面.png)：测试数据清理后的 Dashboard 空状态。
- [KNOWN-ISSUES 索引](./KNOWN-ISSUES.md)：当前状态与两个问题详情。
- [项目说明](../README.md)：安装、组件和运行方式。

交接文档引用的代码结论以当前基线 2722fbcfd10f5784fa19b669bba1f5fe21a69535 为准；两个问题的浏览器复现记录来自测试基线 8cea280c9007d4e924d5705632c6685a80d53da1。后续修复者应在其实际工作分支重新核对代码和问题状态。
