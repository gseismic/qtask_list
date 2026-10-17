import { useEffect, useRef, useState } from "react";
import { api } from "../api";
import { useAppData } from "../appData";
import { useTaskPage } from "../useTaskPage";
import { taskState } from "../taskView";
import type { TaskRow } from "../types";
import { STATE_KEYS, STATE_LABELS, type StateKey } from "../types";
import { DataFreshness, ErrorBanner, Skeleton, StateBadge, fmtTime, shortId } from "./ui";

export interface TaskFilters {
  state: StateKey;
  search: string;
  action?: string;
  createdAfter?: string;
  createdBefore?: string;
  completedAfter?: string;
  completedBefore?: string;
}

export interface TaskFilterBarTimeRange {
  createdStart: string;
  createdEnd: string;
  completedStart: string;
  completedEnd: string;
}

export function TaskFilterBar({ state, onStateChange, search, onSearchChange, timeRange,
  onTimeRangeChange, autoRefresh, onAutoRefreshChange, extra, showStateSelect = true,
  action = "", onActionChange,
}: {
  state: StateKey;
  onStateChange: (state: StateKey) => void;
  search: string;
  onSearchChange: (value: string) => void;
  timeRange: TaskFilterBarTimeRange;
  onTimeRangeChange: (range: TaskFilterBarTimeRange) => void;
  autoRefresh: boolean;
  onAutoRefreshChange: (enabled: boolean) => void;
  extra?: React.ReactNode;
  showStateSelect?: boolean;
  action?: string;
  onActionChange?: (value: string) => void;
}) {
  const [localSearch, setLocalSearch] = useState(search);
  const [localAction, setLocalAction] = useState(action);
  const activeFilters = [action, ...Object.values(timeRange)].filter(Boolean).length;
  const [advanced, setAdvanced] = useState(activeFilters > 0);
  const callbacks = useRef({ onSearchChange, onActionChange });
  callbacks.current = { onSearchChange, onActionChange };
  useEffect(() => setLocalSearch(search), [search]);
  useEffect(() => setLocalAction(action), [action]);
  useEffect(() => {
    if (localSearch === search) return;
    const timer = window.setTimeout(() => callbacks.current.onSearchChange(localSearch), 300);
    return () => window.clearTimeout(timer);
  }, [localSearch, search]);
  useEffect(() => {
    if (localAction === action) return;
    const timer = window.setTimeout(() => callbacks.current.onActionChange?.(localAction.trim()), 300);
    return () => window.clearTimeout(timer);
  }, [localAction, action]);
  const fields = [
    ["createdStart", "发布起"], ["createdEnd", "发布止"],
    ["completedStart", "完成起"], ["completedEnd", "完成止"],
  ] as const;
  return <>
    <div className="filter-bar">
      {extra}
      <input className="input task-search" placeholder="搜索 task_id / action / payload" aria-label="搜索任务"
        value={localSearch} onChange={(event) => setLocalSearch(event.target.value)} />
      {showStateSelect && <select className="input" aria-label="任务状态" value={state}
        onChange={(event) => onStateChange(event.target.value as StateKey)}>
        {STATE_KEYS.map((key) => <option key={key} value={key}>{STATE_LABELS[key]}</option>)}
      </select>}
      <button className="btn" onClick={() => setAdvanced((value) => !value)} aria-expanded={advanced}>
        高级筛选{activeFilters > 0 ? ` (${activeFilters})` : ""}
      </button>
      <span className="spacer" />
      <label className="faint refresh-toggle" title="跟随顶栏刷新间隔；加载更多后暂停以保持当前列表">
        <input type="checkbox" checked={autoRefresh} onChange={(event) => onAutoRefreshChange(event.target.checked)} />
        自动刷新
      </label>
    </div>
    {advanced && <div className="filter-bar advanced-filters" aria-label="高级筛选">
      {onActionChange && <label>精确 action <input className="input" value={localAction}
        onChange={(event) => setLocalAction(event.target.value)} placeholder="例如 fetch_stock" /></label>}
      {fields.map(([key, label]) => <label key={key}>{label} <input className="input" type="datetime-local"
        value={timeRange[key]} onChange={(event) => onTimeRangeChange({ ...timeRange, [key]: event.target.value })} /></label>)}
    </div>}
  </>;
}

export function toUnix(value: string): number | undefined {
  if (!value) return undefined;
  const timestamp = new Date(value).getTime();
  return Number.isFinite(timestamp) ? timestamp / 1000 : undefined;
}

export function TaskTable({ queue, filters, limit = 50, onOpenTask, reloadKey = 0,
  autoRefresh, onPauseAutoRefresh, emptyHint, onExactTaskMatch,
}: {
  queue: string | null;
  filters: TaskFilters;
  limit?: number;
  onOpenTask: (task: TaskRow) => void;
  reloadKey?: number;
  autoRefresh: boolean;
  onPauseAutoRefresh: () => void;
  emptyHint?: React.ReactNode;
  onExactTaskMatch?: (taskId: string) => void;
}) {
  const { interval, refreshVersion } = useAppData();
  const queryKey = JSON.stringify([queue, filters.state, filters.search, filters.action,
    filters.createdAfter, filters.createdBefore, filters.completedAfter, filters.completedBefore, limit]);
  const automatic = autoRefresh && interval !== 0;
  const page = useTaskPage({
    queryKey,
    refreshVersion: refreshVersion + reloadKey,
    interval: automatic ? interval : 0,
    onPause: onPauseAutoRefresh,
    fetchPage: (cursor) => {
      const options = {
        state: filters.state, search: filters.search.trim() || undefined,
        action: filters.action?.trim() || undefined, limit, cursor,
        createdAfter: toUnix(filters.createdAfter ?? ""), createdBefore: toUnix(filters.createdBefore ?? ""),
        completedAfter: toUnix(filters.completedAfter ?? ""), completedBefore: toUnix(filters.completedBefore ?? ""),
      };
      return queue === null ? api.tasks(options) : api.queueTasks({ queue, ...options });
    },
  });
  const searchMatch = useRef({ search: "", opened: false });
  useEffect(() => {
    const search = filters.search.trim();
    if (searchMatch.current.search !== search) searchMatch.current = { search, opened: false };
    if (onExactTaskMatch && search && page.exactTaskId === search && !searchMatch.current.opened) {
      searchMatch.current.opened = true;
      onExactTaskMatch(search);
    }
  }, [filters.search, page.exactTaskId, onExactTaskMatch]);
  if (page.loading && page.rows === null) return <Skeleton lines={10} height={16} />;
  return <>
    {page.error && <ErrorBanner error={page.error} onRetry={page.refresh} />}
    <div className="table-toolbar">
      <button className="btn sm" onClick={page.refresh} disabled={page.loading}>刷新第一页</button>
      <DataFreshness lastUpdated={page.lastUpdated} automatic={automatic} loading={page.loading} error={page.error} />
      <span className="faint">加载更多后暂停自动刷新</span>
    </div>
    <div className="table-wrap">
      <table className="tbl">
        <thead><tr>
          <th>task_id</th><th>action</th>{queue === null && <th>队列</th>}<th>attempt</th>
          <th>状态</th><th>发布时间</th><th>更新时间</th>
        </tr></thead>
        <tbody>{(page.rows ?? []).map((row, index) => {
          const state = taskState(row, String(row._state ?? filters.state));
          return <tr key={row.task_id ?? index} className="rowlink" tabIndex={0}
            onClick={() => onOpenTask(row)} onKeyDown={(event) => {
              if (event.key === "Enter") { event.preventDefault(); onOpenTask(row); }
            }}>
            <td className="mono" title={row.task_id}>{shortId(row.task_id)}</td>
            <td className="mono">{row.action || "—"}</td>
            {queue === null && <td className="mono">{String(row._queue ?? row.queue ?? "—")}</td>}
            <td className="num">{row.attempt ?? "—"}</td>
            <td><StateBadge state={state} label={STATE_LABELS[state as StateKey] ?? (state === "unknown" ? "状态未知" : state)} /></td>
            <td className="num faint">{fmtTime(row.created_at)}</td><td className="num faint">{fmtTime(row.updated_at)}</td>
          </tr>;
        })}</tbody>
      </table>
      {page.rows !== null && page.rows.length === 0 && <div style={{ padding: 24 }}>
        {page.scanLimited ? "本页没有匹配任务，可继续搜索。" : emptyHint ?? "该筛选下没有任务。"}
      </div>}
    </div>
    {page.scanLimited && <p className="muted">本页已达到扫描上限，可继续搜索。</p>}
    {page.nextCursor && <div style={{ marginTop: 10 }}>
      <button className="btn" onClick={page.loadMore} disabled={page.loading || page.loadingMore}>
        {page.loading || page.loadingMore ? "加载中…" : `加载更多（已显示 ${page.rows?.length ?? 0} 条）`}
      </button>
    </div>}
  </>;
}
