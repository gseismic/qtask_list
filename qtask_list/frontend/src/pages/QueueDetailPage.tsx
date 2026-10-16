import { useEffect, useMemo, useState } from "react";
import { Link, useLocation, useSearchParams } from "react-router-dom";
import { api } from "../api";
import type { DiagnoseInfo, QueueStats, TaskRow } from "../types";
import { STATE_KEYS, STATE_LABELS, type StateKey, statForState } from "../types";
import { ConfirmDialog, ErrorBanner, OpsMenu, StateBadge } from "../components/ui";
import { TaskFilterBar, TaskTable, type TaskFilters } from "../components/TaskTable";
import { PushTaskDialog } from "../components/PushTaskDialog";
import { TaskDrawer } from "../components/TaskDrawer";
import { useAppData } from "../appData";

const EMPTY_HINTS: Partial<Record<StateKey, string>> = {
  dlq: "dlq 队列是空的——任务重试耗尽才会进入这里。",
  processing: "当前没有任务在执行。检查 Worker 是否在运行。",
  retry_wait: "没有等待自动重试的任务。任务失败后按退避策略会出现在这里。",
  deadline_missed: "没有过期任务。任务超过执行截止时间（expire）会被标记到这里。",
};

function queueNameFromPath(pathname: string): string {
  const encoded = pathname.slice("/queues/".length);
  try { return decodeURIComponent(encoded); }
  catch { return encoded; }
}

export function QueueDetailPage() {
  const { queues, error, reload } = useAppData();
  // 从原始路径解码一次，保留名称中的字面 %2F，避免路由参数的再次解码。
  const { pathname } = useLocation();
  const queueName = queueNameFromPath(pathname);
  const [params, setParams] = useSearchParams();
  const stateParam = (params.get("state") as StateKey) || "all";

  const stats = (queues?.find((queue) => queue.name === queueName) as unknown as QueueStats | undefined) ?? null;
  const [filters, setFilters] = useState<TaskFilters>({
    state: STATE_KEYS.includes(stateParam) ? stateParam : "all",
    search: "",
    createdAfter: "",
    createdBefore: "",
    completedAfter: "",
    completedBefore: "",
  });
  const [limit, setLimit] = useState(50);
  const [reloadKey, setReloadKey] = useState(0);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [openTask, setOpenTask] = useState<TaskRow | null>(null);
  const [diagnose, setDiagnose] = useState<DiagnoseInfo | null>(null);
  const [pushOpen, setPushOpen] = useState(false);
  const [confirm, setConfirm] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [opError, setOpError] = useState<string | null>(null);

  useEffect(() => {
    if (STATE_KEYS.includes(stateParam)) setFilters((f) => ({ ...f, state: stateParam }));
  }, [stateParam]);

  const loadDiagnose = async () => {
    setOpError(null);
    try {
      const data = await api.diagnose(queueName);
      setDiagnose(data);
    } catch (e) {
      setOpError(e instanceof Error ? e.message : String(e));
    }
  };

  const badges = useMemo(() => {
    if (!stats) return [];
    const items: Array<{ state: StateKey; count: number }> = [];
    if (stats.queue) items.push({ state: "ready", count: stats.queue });
    if (stats.processing) items.push({ state: "processing", count: stats.processing });
    if (stats.retry) items.push({ state: "retry", count: stats.retry });
    if (stats.retry_wait) items.push({ state: "retry_wait", count: stats.retry_wait });
    if (stats.delay) items.push({ state: "delay", count: stats.delay });
    if (stats.dlq) items.push({ state: "dlq", count: stats.dlq });
    if (stats.deadline_missed) items.push({ state: "deadline_missed", count: stats.deadline_missed });
    return items;
  }, [stats]);

  const setFilterState = (state: StateKey) => {
    const next = new URLSearchParams(params);
    next.set("state", state);
    setParams(next, { replace: true });
    setFilters((f) => ({ ...f, state }));
  };

  const runOp = async (fn: () => Promise<unknown>) => {
    if (busy) return;
    setBusy(true);
    setOpError(null);
    try {
      await fn();
      setConfirm(null);
      setReloadKey((k) => k + 1);
      reload();
    } catch (e) {
      setOpError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const totalCount = stats && filters.state !== "all" ? statForState(stats, filters.state) : null;
  const cumulativeCount = ["completed", "failed", "skipped", "cancelled"].includes(filters.state);

  return (
    <div className="page">
      <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
        <h1 className="page-title mono">{queueName}</h1>
        <span style={{ flex: 1 }} />
        <Link to="/queues">← 队列列表</Link>
      </div>
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap", margin: "4px 0 14px" }}>
        {badges.map((b) => (
          <button
            key={b.state}
            className="badge"
            style={{
              background: b.state === "dlq" ? "var(--danger-dim)" : "var(--bg-surface-2)",
              color: b.state === "dlq" ? "var(--c-danger)" : "var(--text-secondary)",
              border: "none",
              cursor: "pointer",
            }}
            onClick={() => setFilterState(b.state)}
            title={`查看 ${STATE_LABELS[b.state]}`}
          >
            {STATE_LABELS[b.state]} {b.count}
          </button>
        ))}
        {stats && (stats.active_workers > 0 || stats.stale_workers > 0) && (
          <span className={`badge ${stats.stale_workers > 0 ? "c-danger" : "c-success"}`}>
            worker {stats.active_workers} 在线{stats.stale_workers ? ` / ${stats.stale_workers} 失联` : ""}
          </span>
        )}
      </div>

      {error && <ErrorBanner error={error} onRetry={reload} />}
      {opError && !confirm && <ErrorBanner error={opError} onRetry={() => setOpError(null)} />}

      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 14 }}>
        <OpsMenu
          label="操作"
          items={[
            { label: "投递任务…", onClick: () => setPushOpen(true) },
            { label: "drain 手动重试（retry → ready）", onClick: () => setConfirm("retry") },
            { label: "重放全部 DLQ", onClick: () => setConfirm("requeueDlq") },
            { label: "恢复失联 processing", onClick: () => setConfirm("recover") },
            { label: "诊断", onClick: loadDiagnose },
            { label: "清理 15 天前历史", onClick: () => setConfirm("cleanHistory") },
          ]}
        />
        <span style={{ flex: 1 }} />
        <OpsMenu
          label="⚠ 危险操作"
          items={[
            { label: "清空队列…", danger: true, onClick: () => setConfirm("clear") },
            { label: "删除队列…", danger: true, onClick: () => setConfirm("delete") },
          ]}
        />
      </div>

      <div className="tabs">
        {STATE_KEYS.map((s) => {
          const count = stats && s !== "all" ? statForState(stats, s) : null;
          const hot = s === "dlq" || s === "deadline_missed";
          return (
            <button
              key={s}
              className={`tab${filters.state === s ? " active" : ""}`}
              onClick={() => setFilterState(s)}
            >
              {STATE_LABELS[s]}
              {count !== null && count > 0 && <span className={`cnt${hot ? " hot" : ""}`}
                title={["completed", "failed", "skipped", "cancelled"].includes(s) ? "监控累计计数，历史保留期可能使可查询记录更少" : undefined}>{count}</span>}
            </button>
          );
        })}
      </div>

      <TaskFilterBar
        showStateSelect={false}
        state={filters.state}
        onStateChange={setFilterState}
        search={filters.search}
        onSearchChange={(s) => setFilters((f) => ({ ...f, search: s }))}
        action={filters.action}
        onActionChange={(action) => setFilters((f) => ({ ...f, action }))}
        timeRange={{
          createdStart: filters.createdAfter ?? "",
          createdEnd: filters.createdBefore ?? "",
          completedStart: filters.completedAfter ?? "",
          completedEnd: filters.completedBefore ?? "",
        }}
        onTimeRangeChange={(t) => setFilters((f) => ({ ...f, createdAfter: t.createdStart, createdBefore: t.createdEnd,
          completedAfter: t.completedStart, completedBefore: t.completedEnd }))}
        autoRefresh={autoRefresh}
        onAutoRefreshChange={setAutoRefresh}
      />

      {totalCount !== null && (
        <div className="faint" style={{ marginBottom: 6 }}>
          {cumulativeCount ? "该结果累计" : "当前该状态"} {totalCount.toLocaleString("zh-CN")} 条
          {cumulativeCount && "；下面显示当前保留的任务记录"}
        </div>
      )}

      <TaskTable
        queue={queueName}
        filters={filters}
        limit={limit}
        onLimitChange={setLimit}
        onOpenTask={setOpenTask}
        reloadKey={reloadKey}
        autoRefresh={autoRefresh}
        onPauseAutoRefresh={() => setAutoRefresh(false)}
        emptyHint={
          <div>
            <p>{EMPTY_HINTS[filters.state] ?? "该筛选下没有任务。"}</p>
            {filters.state !== "all" && (
              <button className="btn sm" onClick={() => setFilterState("all")}>
                查看 {STATE_LABELS.all}
              </button>
            )}
          </div>
        }
      />

      {diagnose && (
        <div className="overlay" style={{ justifyContent: "center" }} onClick={() => setDiagnose(null)}>
          <div className="dialog" style={{ width: 560 }} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ marginTop: 0 }}>诊断 · {queueName}</h3>
            <ul>
              {diagnose.suggestions.map((s, i) => (
                <li key={i}>{s}</li>
              ))}
            </ul>
            {diagnose.workers.length > 0 && (
              <>
                <div className="faint" style={{ marginTop: 10 }}>Worker</div>
                {diagnose.workers.map((w) => (
                  <div key={`${w.queue}:${w.worker_id}`} style={{ display: "flex", gap: 8, alignItems: "center", padding: "4px 0" }}>
                    <span className="mono">{w.worker_id}</span>
                    <StateBadge state={w.active ? "processing" : "failed"} label={w.active ? "在线" : "失联"} />
                    <span className="faint num">processing {w.processing}</span>
                  </div>
                ))}
              </>
            )}
            <div style={{ textAlign: "right", marginTop: 12 }}>
              <button className="btn" onClick={() => setDiagnose(null)}>关闭</button>
            </div>
          </div>
        </div>
      )}

      {pushOpen && (
        <PushTaskDialog queue={queueName} open onClose={() => setPushOpen(false)} onPushed={() => { setReloadKey((k) => k + 1); reload(); }} />
      )}

      {openTask?.task_id && (
        <TaskDrawer
          key={openTask.task_id}
          taskId={openTask.task_id}
          stateHint={String(openTask._state ?? openTask.state ?? "")}
          onClose={() => setOpenTask(null)}
          onChanged={() => { setReloadKey((k) => k + 1); reload(); }}
          onOpenTask={(taskId) => setOpenTask({ task_id: taskId })}
        />
      )}

      <ConfirmDialog
        open={confirm === "retry"}
        error={opError}
        title="drain 手动重试"
        confirmText="执行"
        busy={busy}
        body={<p>retry 队列中的任务将移回 ready。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => runOp(() => api.retryQueue(queueName))}
      />
      <ConfirmDialog
        open={confirm === "requeueDlq"}
        error={opError}
        title="重放全部 DLQ"
        confirmText="重放"
        busy={busy}
        body={<p>DLQ 中所有任务将重置重试计数并回到 ready。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => runOp(() => api.requeueDlq(queueName, null))}
      />
      <ConfirmDialog
        open={confirm === "recover"}
        error={opError}
        title="恢复失联 processing"
        confirmText="恢复"
        busy={busy}
        body={<p>将把失联 Worker processing 中的任务移回主队列。活跃 Worker 不受影响。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => runOp(() => api.recoverQueue(queueName, false))}
      />
      <ConfirmDialog
        open={confirm === "cleanHistory"}
        error={opError}
        title="清理 15 天前历史"
        confirmText="清理"
        busy={busy}
        body={<p>将删除该队列 15 天之前的历史记录（含任务 hash 与索引）。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => runOp(() => api.cleanHistory(queueName, 15))}
      />
      <ConfirmDialog
        open={confirm === "clear"}
        error={opError}
        title={`清空队列 ${queueName}`}
        danger
        confirmKeyword={queueName}
        confirmText="清空"
        busy={busy}
        body={<p>将清空 ready/retry/delay/dlq 中所有消息，保留 processing 与历史。此操作不可恢复。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => runOp(() => api.clearQueue(queueName, true, false, false))}
      />
      <ConfirmDialog
        open={confirm === "delete"}
        error={opError}
        title={`删除队列 ${queueName}`}
        danger
        confirmKeyword={queueName}
        confirmText="删除"
        busy={busy}
        body={<p>将删除该队列全部数据（含历史索引）。请确认没有 Worker 正在使用。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => runOp(async () => {
          await api.deleteQueue(queueName);
          window.location.href = "/queues";
        })}
      />
    </div>
  );
}
