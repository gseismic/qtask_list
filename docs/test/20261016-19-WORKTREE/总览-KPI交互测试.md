# 总览 KPI 交互测试

- 测试开始时间：2026-10-16 19:12（Asia/Shanghai）。
- 测试结束时间：2026-10-16 19:55（Asia/Shanghai）。
- 实际模型：Codex，环境声明基于 GPT-6；具体模型 ID 未提供，记为 model-unknown。
- 测试范围：WORKTREE；基准提交 `3ff689706e788264b02839c4e2db9bd8a4644d50`。修改后未提交。
- 相关工作区文件：`qtask_list/frontend/src/pages/OverviewPage.tsx`、`qtask_list/frontend/src/components/ui.tsx`、`qtask_list/frontend/src/styles.css`；构建产物 `qtask_list/dashboard/static/spa/index.html`、`qtask_list/dashboard/static/spa/assets/index-5vhg9Z4O.js`、`qtask_list/dashboard/static/spa/assets/index-B0zgLNSh.css`；隔离数据启动脚本 `tests/manual/handoff_dashboard_server.py`。工作区还有其他先前改动，本次测试未覆盖或改动。
- 关联计划：[PLAN-20261016-003](../../plans/PLAN-20261016-003-model-unknown-overview-kpi-interactions.md)。

## 目的与环境

复现总览 KPI 卡“点击没有反应”的反馈，修正交互后核对下钻、异常定位和总览原有点击行为。

浏览器通过 Playwright CLI 0.1.21 启动 Chrome 会话。Dashboard 地址为 `http://127.0.0.1:56643`，数据来自 `tests/manual/handoff_dashboard_server.py` 创建的私有 Redis；样例队列为 `handoff_browser:search`、`handoff_browser:external` 和 `handoff_browser:registered-empty`。未连接或修改共享 Redis。运行信息见[浏览器运行信息](./browser-runtime.json)。

构建命令：在 `qtask_list/frontend/` 执行 `pnpm build`（含 `tsc --noEmit` 和 Vite 生产构建）。随后用 Playwright CLI 打开隔离 Dashboard，点击 KPI、矩阵分组、筛选和告警定位，并读取页面 URL、筛选状态、可见队列和浏览器控制台。

## 结果

| 操作 | 预期 | 实际 | 结果与证据 |
| --- | --- | --- | --- |
| 修复前点击“活跃队列” | 下钻到队列页 | 留在 `/`，矩阵筛选改为“进行中”，从 3 个队列显示为 2 个 | 事件可用，但行为不符合页面规格；[修复前快照](./page-2026-10-16T11-12-43-522Z.yml)。 |
| 点击“活跃队列” | 打开 `/queues?filter=active` | 即使浏览器保留了“进行中”和分组折叠状态，仍进入队列页并选中“进行中”，只列出 2 条有待处理的队列 | 通过；[快照](./page-2026-10-16T11-16-52-626Z.yml)。 |
| 点击“总待处理” | 打开 `/queues?filter=active` | URL 与筛选均正确 | 通过；[快照](./page-2026-10-16T11-17-23-703Z.yml)。 |
| hover / 键盘 focus 可点击 KPI | 显示对应操作提示 | hover 后 opacity 为 1；Tab 聚焦后 `:focus-visible=true`，过渡完成后 opacity 为 1，按钮可访问名称包含操作 | 通过；[hover 快照](./page-2026-10-16T11-18-00-965Z.yml)、[键盘焦点结果](./keyboard-focus-review.txt)。 |
| 点击“异常队列” | 留在总览，筛选“仅异常”，展开有结果的分组并定位矩阵 | 筛选正确，namespace 展开，列出 2 条异常队列；矩阵标题位于视口内 | 通过；[快照](./page-2026-10-16T11-17-47-318Z.yml)。 |
| 折叠矩阵分组 | 隐藏该组队列行 | `aria-expanded=false`，可见队列行从 2 变为 0 | 通过；[快照](./page-2026-10-16T11-18-12-701Z.yml)。 |
| 告警“定位” | 跳转至队列详情并定位 ready 状态 | URL 为 `/queues/handoff_browser%3Aexternal?state=ready` | 通过；[快照](./page-2026-10-16T11-18-21-297Z.yml)。 |
| 合计观测速率卡 | 保持说明性、不可点击 | 页面元素不是按钮 | 通过。 |

`pnpm build` 成功（56 个模块，TypeScript 检查通过）；浏览器控制台 0 errors、0 warnings。未运行前端单元测试套件；本次按用户要求执行隔离浏览器交互复现与复测。

截图未单独保存；交互前后页面快照均放在本目录。测试数据由私有 Redis 提供，Dashboard 和 Redis 在测试记录完成后继续运行供用户复测。

目录中另有 `overview-initial.yml`、`overview-kpi-review.txt`、`overview-reviewed.yml`、`browser-console.txt` 和 `processing-index-review.json` 等文件；本轮未覆盖这些已有记录。
