# 普通关键词搜索误触发 task_id 详情请求

## 创建事件

- ID：KI-20261016-1218-002
- 标题：普通关键词搜索误触发 task_id 详情请求
- 初始状态：未解决
- 严重度：P3
- 创建时间：2026-10-16 12:18（Asia/Shanghai）
- 模型：Codex / model-unknown（环境仅说明基于 GPT-6，未公开具体模型 ID）
- 确认版本：8cea280c9007d4e924d5705632c6685a80d53da1
- 引入提交：未知，尚未定位
- 来源：完整浏览器测试报告 [测试报告](../test/20261016-10-8cea280/测试报告.md)，并核对 [TasksPage.tsx](../../qtask_list/frontend/src/pages/TasksPage.tsx)。

### 问题描述

任务列表的关键词输入会触发 task_id 自动打开逻辑。当前判断只要求字符串由字母、数字、下划线或连字符组成且长度至少为 8；符合条件的普通 action/payload 关键词也会触发任务详情请求。

### 触发条件

在任务搜索框输入类似 browser_test_dlq 的普通关键词（至少 8 个允许字符），页面同时调用任务详情接口 /api/task/{keyword}。确认版本代码约第 38–45 行包含此判断和请求。

### 影响

关键词列表搜索结果仍能正常显示，但详情接口会返回 404，造成无效网络请求和浏览器控制台错误。本轮复现记录到两次 404。

### 确认依据

- 完整浏览器测试基线：8cea280c9007d4e924d5705632c6685a80d53da1。
- 复现证据：[任务搜索快照](../test/20261016-10-8cea280/tasks-search.yml)及测试报告。
- 代码依据：qtask_list/frontend/src/pages/TasksPage.tsx 的 task_id 格式启发式判断会对匹配词调用 api.task。

### 临时绕过方式

使用已知的精确 task_id 打开任务详情；按 action 检索时可使用页面的精确 action 筛选项，避免将普通关键词解释为 task_id。

## 2026-10-16 13:12（Asia/Shanghai）处理与验证事件

- 模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 状态：未解决 → 已解决。依据：普通关键词不再发送详情请求，真实新旧 ID 自动打开和并发查询切换均通过回归。
- 修复版本：WORKTREE；基准提交 3ff689706e788264b02839c4e2db9bd8a4644d50。相关未提交文件：qtask_list/admin.py、qtask_list/frontend/src/{components/TaskTable.tsx,pages/TasksPage.tsx,types.ts,useTaskPage.ts}、tests/test_dashboard_api.py、tests/manual/handoff_tasks_search.js 和 handoff_dashboard_server.py，以及更新的 SPA index.html/JS 产物。内容尚未固定为提交；引入提交仍未知。
- 处理：列表响应新增可空 exact_task_id，在第一页按真实任务记录确认精确命中；前端使用当前查询的确认结果打开详情，移除格式猜测和吞掉 404 的预请求。没有限制为 UUID，保留短 ID、旧字符串 ID 和直接定位不依赖历史索引/列表筛选的行为。关闭详情后刷新和轮询不会重开，旧响应不能干扰新查询。
- 验证：普通 action 关键词、不存在的普通 ID/UUID 均无详情请求；有效 UUID、legacy_order_42、v1、旧任务-42、指定队列及无索引 ID 正常打开。最终真实浏览器搜索脚本 15 项断言通过，0 请求失败、0 页面异常、0 控制台错误；前端 15 项行为测试及构建通过；最终全量 Python 246 passed。
- 关联计划：[PLAN-20261016-001-model-unknown-dashboard-handoff-fixes](../plans/PLAN-20261016-001-model-unknown-dashboard-handoff-fixes.md)；[结果](../plans/PLAN-20261016-001-model-unknown-dashboard-handoff-fixes-OUTCOME.md)；[测试记录](../test/20261016-13-WORKTREE/测试记录.md)。
- 交付边界：修复已在工作区验证；原有 8766 Dashboard 未重启，重启后加载 Python 修复。没有自动提交或推送。

## 2026-10-17 18:17（Asia/Shanghai）补记实际修复提交

- 模型：Codex / model-unknown（环境声明基于 GPT-6，未提供具体模型 ID）。
- 来源：用户明确要求“提交代码”，相关实现及验收记录已提交。
- 状态保持已解决；最终修复版本：`a1db46ff6de872de5f115d16af85e9b264857329`。引入提交仍未定位。
- 最终实现由列表确认 exact_task_id，前端不再按格式猜测并预请求详情。
- 当前整体验收：Python 293 项、前端 20 项、浏览器 41 组通过，ruff/mypy/构建、独立 wheel 启动及产物一致性通过；详见[最终验证](../test/20261017-17-WORKTREE/管理后台修复验证.md)。
- 本次只补充提交依据，保留历史 WORKTREE 来源与测试时点；未推送。
