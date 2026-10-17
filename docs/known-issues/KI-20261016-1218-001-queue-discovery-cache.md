# 外部清空后 Dashboard 仍展示已删除的旧队列

## 创建事件

- ID：KI-20261016-1218-001
- 标题：外部清空后 Dashboard 仍展示已删除的旧队列
- 初始状态：未解决
- 严重度：P2
- 创建时间：2026-10-16 12:18（Asia/Shanghai）
- 模型：Codex / model-unknown（环境仅说明基于 GPT-6，未公开具体模型 ID）
- 确认版本：8cea280c9007d4e924d5705632c6685a80d53da1
- 引入提交：未知，尚未定位
- 来源：完整浏览器测试报告 [测试报告](../test/20261016-10-8cea280/测试报告.md)，并核对 [qtask_list/admin.py](../../qtask_list/admin.py) 的 queue_names 实现。

### 问题描述

Dashboard 进程运行期间，如果 Redis 队列由另一个进程清理，队列发现缓存不会立即失效。Redis 注册表和实际队列数据已为空时，Dashboard 的队列 API 仍可能返回进程之前发现的旧版队列名，页面因此显示并不存在的空队列。

### 触发条件

Dashboard 已扫描过旧版队列并缓存发现结果；在缓存刷新前，外部进程删除这些队列及历史数据。queue_names 在缓存有效期内会把当前注册表结果与旧缓存合并返回（确认版本代码约第 91–121 行）。

### 影响

操作人员可能误以为队列仍存在，无法从 Dashboard 确认清理结果。本轮复现时 Redis 注册表为 0，Dashboard API/界面仍显示 7 个空队列；Redis 数据本身已经清空。

### 确认依据

- 完整浏览器测试基线：8cea280c9007d4e924d5705632c6685a80d53da1。
- 复现证据：[缓存异常截图](../test/20261016-10-8cea280/缓存异常复现.png)及测试报告。
- 代码依据：qtask_list/admin.py 的 queue_names 将 qtask:queues 注册表与限时缓存的旧队列发现结果合并。

### 临时绕过方式

重启 Dashboard 进程可清除其进程内缓存，之后队列列表恢复为空。此方法只规避界面旧数据，不代表缓存失效行为已修复。

## 2026-10-16 13:12（Asia/Shanghai）处理与验证事件

- 模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 状态：未解决 → 已解决。依据：完成外部清理后的同实例 API 和真实浏览器回归。
- 修复版本：WORKTREE；基准提交 3ff689706e788264b02839c4e2db9bd8a4644d50。相关未提交文件：qtask_list/admin.py、tests/test_dashboard_api.py；测试环境脚本 tests/manual/handoff_dashboard_server.py。内容尚未固定为提交；引入提交仍未知。
- 处理：读取缓存时批量核实 ready/retry/dlq/delay/processing/历史索引及已知专属 processing 键；已知键全部消失时，所有失效候选共用一次 processing 补查，保留状态迁移后的真实旧队列。无有效数据的候选从本进程缓存移除；旧版新队列的每小时发现和注册表空队列语义保持不变。
- 验证：外部删除七类旧队列存储、ready 转移到各状态、Worker 专属键切换、批量失效扫描次数、历史保留和注册表空队列回归通过。最终全量 Python 246 passed。最终浏览器进程内外部删除无注册表旧队列 3→2，删除另一旧队列及历史 2→1，私有 Redis 外部清空后 API/页面均为 0；没有重启进程或等待缓存超时。
- 关联计划：[PLAN-20261016-001-model-unknown-dashboard-handoff-fixes](../plans/PLAN-20261016-001-model-unknown-dashboard-handoff-fixes.md)；[结果](../plans/PLAN-20261016-001-model-unknown-dashboard-handoff-fixes-OUTCOME.md)；[测试记录](../test/20261016-13-WORKTREE/测试记录.md)。
- 交付边界：修复已在工作区验证；原有 8766 Dashboard 未重启，重启后加载修复。没有自动提交或推送。

## 2026-10-16 18:58（Asia/Shanghai）按用户新要求简化发现

- 模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。状态保持已解决。
- 来源：用户明确“不需要兼容性，直接按最合适的来”；该要求取代前次保留 V1 自动发现的设计约束。
- 实现版本：WORKTREE，基准提交 3ff689706e788264b02839c4e2db9bd8a4644d50。相关未提交文件：qtask_list/admin.py、state.py、worker.py、tests/test_registry_discovery.py、test_dashboard_api.py、test_cli.py、tests/manual/handoff_dashboard_server.py、README.md。尚未固定为提交，引入提交仍未知。
- 替代实现：queue_names 仅读取 qtask:queues，list_workers 仅读取相应 Worker 注册表；删除前次兼容扫描、小时缓存及失效校验。未注册状态键或历史不再加入全局列表。心跳原子补齐队列/Worker 登记，退出时保留有残留 processing 的 Worker，失效清理原子保护并发续租。
- 验证：最终全量 Python 243 passed，前端 15 项、浏览器 18 项通过，ruff/mypy 通过。新测试禁止发现路径执行 SCAN，并覆盖外部注册项增删、残留数据不触发发现、空闲 Worker、孤儿恢复、类型错误与并发 namespace/续租。浏览器同进程外部移除注册项 3→2、重新登记 2→3、注册表清空 3→0 均即时生效，即使消息与历史仍存在。
- 关联：[计划](../plans/PLAN-20261016-002-model-unknown-registry-discovery.md)、[结果](../plans/PLAN-20261016-002-model-unknown-registry-discovery-OUTCOME.md)、[测试记录](../test/20261016-18-WORKTREE/测试记录.md)。原 13:12 事件保留为历史，当前发现行为以本事件为准。现有 8766 Dashboard 未重启，需重启加载修改；未提交、未推送。

## 2026-10-17 18:17（Asia/Shanghai）补记实际修复提交

- 模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 来源：用户明确要求“提交代码”，相关实现及验收记录已提交。
- 状态保持已解决；最终修复版本：`a1db46ff6de872de5f115d16af85e9b264857329`。引入提交仍未定位。
- 最终实现采用注册表唯一发现，早期兼容扫描方案已由 PLAN-20261016-002 替代。
- 当前整体验收：Python 293 项、前端 20 项、浏览器 41 组通过，ruff/mypy/构建、独立 wheel 启动及产物一致性通过；详见[最终验证](../test/20261017-17-WORKTREE/管理后台修复验证.md)。
- 本次只补充提交依据，保留历史 WORKTREE 来源与测试时点；未推送。
