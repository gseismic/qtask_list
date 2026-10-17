import { useCallback, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useAppData } from "../appData";
import type { TaskRow } from "../types";
import { STATE_KEYS, type StateKey } from "../types";
import { TaskFilterBar, TaskTable, type TaskFilters } from "../components/TaskTable";
import { TaskDrawer } from "../components/TaskDrawer";

export function TasksPage() {
  const { queues, reload } = useAppData();
  const [params, setParams] = useSearchParams();
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [openTask, setOpenTask] = useState<TaskRow | null>(null);
  const queue = params.get("queue") || "";
  const filters: TaskFilters = useMemo(() => {
    const state = (params.get("state") ?? "all") as StateKey;
    return {
      state: STATE_KEYS.includes(state) ? state : "all", search: params.get("q") ?? "",
      action: params.get("action") ?? "", createdAfter: params.get("created_after") ?? "",
      createdBefore: params.get("created_before") ?? "", completedAfter: params.get("completed_after") ?? "",
      completedBefore: params.get("completed_before") ?? "",
    };
  }, [params]);
  const updateParams = useCallback((patch: Record<string, string>) => {
    setParams((current) => {
      const next = new URLSearchParams(current);
      for (const [key, value] of Object.entries(patch)) {
        if (value && !(key === "state" && value === "all")) next.set(key, value);
        else next.delete(key);
      }
      return next;
    }, { replace: true });
  }, [setParams]);
  const queueNames = useMemo(() => (queues ?? []).map((item) => String(item.name)), [queues]);

  const openTaskById = useCallback((id: string) => setOpenTask({ task_id: id }), []);

  return <div className="page">
    <h1 className="page-title">任务</h1>
    <p className="page-desc">跨队列排查任务。task_id 精确命中时直接打开详情；高级筛选会保存在地址中。</p>
    <TaskFilterBar state={filters.state} onStateChange={(state) => updateParams({ state })}
      search={filters.search} onSearchChange={(q) => updateParams({ q })}
      action={filters.action} onActionChange={(action) => updateParams({ action })}
      timeRange={{ createdStart: filters.createdAfter ?? "", createdEnd: filters.createdBefore ?? "",
        completedStart: filters.completedAfter ?? "", completedEnd: filters.completedBefore ?? "" }}
      onTimeRangeChange={(range) => updateParams({ created_after: range.createdStart, created_before: range.createdEnd,
        completed_after: range.completedStart, completed_before: range.completedEnd })}
      autoRefresh={autoRefresh} onAutoRefreshChange={setAutoRefresh}
      extra={<select className="input" aria-label="任务队列" value={queue} onChange={(event) => updateParams({ queue: event.target.value })}>
        <option value="">全部队列</option>
        {queue && !queueNames.includes(queue) && <option value={queue}>{queue}</option>}
        {queueNames.map((name) => <option key={name} value={name}>{name}</option>)}
      </select>} />
    <TaskTable queue={queue || null} filters={filters}
      onOpenTask={(task) => task.task_id && setOpenTask(task)} autoRefresh={autoRefresh}
      onExactTaskMatch={openTaskById}
      onPauseAutoRefresh={() => setAutoRefresh(false)} />
    {openTask?.task_id && <TaskDrawer key={openTask.task_id} taskId={openTask.task_id}
      stateHint={String(openTask._state ?? openTask.state ?? "")}
      onClose={() => setOpenTask(null)} onChanged={reload}
      onOpenTask={openTaskById} />}
  </div>;
}
