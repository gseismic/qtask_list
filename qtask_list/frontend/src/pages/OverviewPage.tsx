import { useMemo } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAppData } from "../appData";
import { fmtInt, fmtRate } from "../sampler";
import { useStoredPreference } from "../preferences";
import { EmptyState, ErrorBanner, KpiCard, QueueComposition, Skeleton } from "../components/ui";

type FilterKey = "all" | "active" | "abnormal";
interface OverviewPreference {
  namespace: string;
  search: string;
  filter: FilterKey;
  collapsed: Record<string, boolean>;
}
const DEFAULT_PREFERENCE: OverviewPreference = { namespace: "*", search: "", filter: "all", collapsed: {} };
function validPreference(value: unknown): value is OverviewPreference {
  if (!value || typeof value !== "object") return false;
  const item = value as OverviewPreference;
  return typeof item.namespace === "string" && typeof item.search === "string" &&
    ["all", "active", "abnormal"].includes(item.filter) && Boolean(item.collapsed) &&
    typeof item.collapsed === "object" && !Array.isArray(item.collapsed) && Object.values(item.collapsed).every((entry) => typeof entry === "boolean");
}
const namespaceOf = (name: string) => name.includes(":") ? name.slice(0, name.indexOf(":")) : "";

export function OverviewPage() {
  const navigate = useNavigate();
  const { queues, progress: queueProgress, alerts, error, loading, reload } = useAppData();
  const [preference, setPreference] = useStoredPreference("qtask.overview.preferences.v1", DEFAULT_PREFERENCE, validPreference);
  const rows = useMemo(() => (queues ?? []).map((item) => {
    const name = String(item.name);
    const stats = item as unknown as Record<string, number>;
    const progress = queueProgress[name];
    const noWorker = (stats.queue ?? 0) > 0 && (stats.active_workers ?? 0) === 0;
    const scheduledOnly = (stats.queue ?? 0) === 0 && (stats.processing ?? 0) === 0 &&
      (stats.retry ?? 0) === 0 && (stats.delay ?? 0) > 0;
    const abnormal = (stats.dlq ?? 0) > 0 || progress.stalled || noWorker ||
      (stats.stale_workers ?? 0) > 0 || (stats.deadline_missed ?? 0) > 0;
    return { name, stats, progress, noWorker, scheduledOnly, abnormal };
  }), [queues, queueProgress]);
  const namespaces = [...new Set(rows.map((row) => namespaceOf(row.name)))].sort();
  const scopedRows = rows.filter((row) =>
    (preference.namespace === "*" || namespaceOf(row.name) === preference.namespace) &&
    row.name.toLowerCase().includes(preference.search.trim().toLowerCase()));
  const groups = new Map<string, typeof rows>();
  for (const row of scopedRows) {
    if (preference.filter === "active" && row.progress.remaining === 0) continue;
    if (preference.filter === "abnormal" && !row.abnormal) continue;
    const namespace = namespaceOf(row.name);
    const group = groups.get(namespace) ?? [];
    group.push(row);
    groups.set(namespace, group);
  }
  // 正常队列按名称固定排列，避免待处理数量每次变化都让盯盘位置跳动。
  const visibleGroups = [...groups.entries()].sort(([left], [right]) => left.localeCompare(right));
  for (const [, group] of visibleGroups) group.sort((left, right) =>
    left.abnormal === right.abnormal ? left.name.localeCompare(right.name) : left.abnormal ? -1 : 1);
  const totalRemaining = scopedRows.reduce((sum, row) => sum + row.progress.remaining, 0);
  const activeCount = scopedRows.filter((row) => row.progress.remaining > 0).length;
  const abnormalCount = scopedRows.filter((row) => row.abnormal).length;
  const sampledRows = scopedRows.filter((row) => row.progress.ratePerMin !== null);
  const totalRate = sampledRows.reduce((sum, row) => sum + (row.progress.ratePerMin ?? 0), 0);
  const visibleAlerts = alerts.filter((alert) => alert.rule === "redismem" || scopedRows.some((row) =>
    alert.locate.queue === row.name || alert.target === row.name || alert.target.startsWith(`${row.name} /`)));
  const setFilter = (filter: FilterKey) => setPreference((previous) => ({ ...previous, filter }));

  return <div className="page">
    <h1 className="page-title">总览</h1>
    <p className="page-desc">查看当前待处理量与异常；完成量和速率来自本次访问近一小时的浏览器观测，切换页面会保留采样。</p>
    {error && <ErrorBanner error={error} onRetry={reload} />}
    {loading && !queues ? <Skeleton lines={6} height={40} /> : !queues ?
      <EmptyState icon="⚠" title="监控数据不可用">连接恢复后点击顶栏“刷新”。</EmptyState> : queues.length === 0 ?
      <EmptyState icon="📭" title="还没有任何队列">用 <code>qtask push</code> 投递任务后，这里会出现运行总览。</EmptyState> : <>
      <div className="filter-bar">
        <select className="input" aria-label="总览 namespace" value={preference.namespace}
          onChange={(event) => setPreference((previous) => ({ ...previous, namespace: event.target.value }))}>
          <option value="*">全部 namespace</option>
          {preference.namespace !== "*" && !namespaces.includes(preference.namespace) && <option value={preference.namespace}>{preference.namespace || "(默认)"}</option>}
          {namespaces.map((namespace) => <option key={namespace} value={namespace}>{namespace || "(默认)"}</option>)}
        </select>
        <input className="input task-search" placeholder="搜索队列名" value={preference.search}
          onChange={(event) => setPreference((previous) => ({ ...previous, search: event.target.value }))} />
        <span className="faint">当前范围 {scopedRows.length} / {rows.length} 个队列</span>
      </div>
      <div className="kpi-row">
        <KpiCard label="活跃队列" value={`${activeCount}/${scopedRows.length}`} hint="当前范围内有待处理任务的队列"
          onClick={() => setFilter("active")} />
        <KpiCard label="总待处理" value={fmtInt(totalRemaining)} hint="排队 + 执行 + 延迟 + 旧重试" onClick={() => setFilter("active")} />
        <KpiCard label="合计观测速率" value={fmtRate(sampledRows.length ? totalRate : null)}
          hint={sampledRows.length ? `已采样 ${sampledRows.length}/${scopedRows.length} 个队列 · 条/min` : "观测至少 30 秒后估算"} />
        <KpiCard label="异常队列" value={abnormalCount} danger={abnormalCount > 0} hint="死信 / 无 Worker / 停滞 / 失联 / 过期"
          onClick={() => setFilter("abnormal")} />
      </div>
      {visibleAlerts.length > 0 && <div className="card overview-alerts">
        <div className="section-head"><h2>待处理告警</h2><Link to="/alerts">查看全部 →</Link></div>
        {visibleAlerts.slice(0, 3).map((alert) => <div key={alert.key} className="overview-alert-row">
          <span className={`badge ${alert.severity === "danger" ? "c-danger" : "c-warning"}`}>{alert.ruleLabel}</span>
          <strong title={alert.target}>{alert.target}</strong><span className="muted">{alert.detail}</span>
          <Link to={alert.locate.page === "queue" && alert.locate.queue ?
            `/queues/${encodeURIComponent(alert.locate.queue)}?state=${alert.locate.state ?? "all"}` : "/workers"}>定位</Link>
        </div>)}
      </div>}
      <div className="section-head">
        <h2>队列运行矩阵</h2>
        <div className="seg">{([["all", "全部"], ["active", "进行中"], ["abnormal", "仅异常"]] as const).map(([key, label]) =>
          <button key={key} className={preference.filter === key ? "on" : ""} onClick={() => setFilter(key)}>{label}</button>)}</div>
      </div>
      <p className="faint composition-legend">待处理构成：<i className="seg-ready" /> 排队 <i className="seg-processing" /> 执行 <i className="seg-delay" /> 延迟 <i className="seg-retry" /> 旧重试 · ETA 仅估算无延迟队列
        <span className="matrix-scroll-hint">· 横向滚动查看完整表格</span>
      </p>
      <div className="matrix-scroll" aria-label="队列运行矩阵，可横向滚动" tabIndex={0}><div className="matrix-content">
        <div className="matrix-columns" aria-hidden="true"><span>队列</span><span>待处理构成</span><span>待处理</span>
          <span>观测完成</span><span>条/min</span><span>预计/状态</span><span>异常</span></div>
        {visibleGroups.map(([namespace, group]) => {
          const abnormal = group.filter((row) => row.abnormal).length;
          const remaining = group.reduce((sum, row) => sum + row.progress.remaining, 0);
          const done = group.every((row) => row.progress.completedObserved !== null) ?
            group.reduce((sum, row) => sum + (row.progress.completedObserved ?? 0), 0) : null;
          const saved = preference.collapsed[namespace];
          const collapsed = typeof saved === "boolean" ? saved : abnormal === 0 && preference.namespace === "*" && !preference.search;
          return <div className="matrix-group" key={namespace}>
            <button className="group-head" aria-expanded={!collapsed} onClick={() => setPreference((previous) => ({ ...previous,
              collapsed: { ...previous.collapsed, [namespace]: !collapsed } }))}>
              <span>{collapsed ? "▸" : "▾"}</span><span>{namespace || "(默认)"}</span>
              <span className="muted">· {group.length} 个队列</span><span className="muted num">· 待处理 {fmtInt(remaining)}</span>
              <span className="muted num">· 观测完成 {fmtInt(done)}</span>
              {abnormal > 0 && <span className="badge c-danger">⚠ {abnormal}</span>}
              {remaining === 0 && abnormal === 0 && <span className="badge c-muted">空闲</span>}
            </button>
            {!collapsed && group.map((row) => <div key={row.name} className={`matrix-row${row.abnormal ? " abnormal" : ""}`}
              role="link" tabIndex={0} onClick={() => navigate(`/queues/${encodeURIComponent(row.name)}`)}
              onKeyDown={(event) => { if (event.key === "Enter") navigate(`/queues/${encodeURIComponent(row.name)}`); }}>
              <span className="qname" title={row.name}>{row.name}</span><QueueComposition stats={row.stats} />
              <span className="num">{fmtInt(row.progress.remaining)}</span><span className="num">{fmtInt(row.progress.completedObserved)}</span>
              <span className="num">{fmtRate(row.progress.ratePerMin)}</span>
              <span className="num" style={{ color: row.progress.stalled ? "var(--c-danger)" : undefined }}>
                {row.noWorker ? "无 Worker" : row.progress.stalled ? "疑似停滞 ⚠" : row.scheduledOnly ?
                  row.stats.retry_wait > 0 ? "等待重试" : "等待调度" : row.progress.etaText ?? "—"}
              </span>
              <span>
                {row.stats.dlq > 0 && <span className="badge c-danger">DLQ {row.stats.dlq}</span>}
                {row.stats.stale_workers > 0 && <span className="badge c-danger">失联 {row.stats.stale_workers}</span>}
                {row.stats.deadline_missed > 0 && <span className="badge c-warning">过期 {row.stats.deadline_missed}</span>}
                {row.noWorker && <span className="badge c-danger">无 Worker</span>}
                {row.stats.observation_indexed === 0 && <span className="badge c-warning" title="旧队列需运行 rebuild-observation 回填监控索引">指标需回填</span>}
                {!row.abnormal && row.progress.remaining > 0 && <span className="badge c-primary">{row.scheduledOnly ? "等待" : "进行中"}</span>}
                {!row.abnormal && row.progress.remaining === 0 && <span className="badge c-success">空闲</span>}
              </span>
            </div>)}
          </div>;
        })}
      </div></div>
      {visibleGroups.length === 0 && <EmptyState icon="🔍" title="当前筛选下没有队列">
        <button className="btn" onClick={() => setPreference((previous) => ({ ...previous, namespace: "*", search: "", filter: "all" }))}>清空筛选</button>
      </EmptyState>}
    </>}
  </div>;
}
