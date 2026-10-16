import { useEffect, useRef, useState } from "react";
import { api } from "../api";
import { useAppData } from "../appData";
import { usePolling } from "../hooks";
import { canReplayTask, replayIds, taskDeadline, taskError, taskState } from "../taskView";
import type { TaskRow } from "../types";
import { STATE_LABELS, type StateKey } from "../types";
import { ConfirmDialog, DataFreshness, ErrorBanner, Skeleton, StateBadge, fmtTime, shortId } from "./ui";

const TERMINAL = new Set(["completed", "failed", "skipped", "cancelled"]);
const DELETABLE = new Set(["ready", "retry", "retry_wait", "delay", "dlq", "deadline_missed", ...TERMINAL]);

export function TaskDrawer({ taskId, stateHint, onClose, onChanged, onOpenTask }: {
  taskId: string;
  stateHint?: string;
  onClose: () => void;
  onChanged: () => void;
  onOpenTask: (id: string) => void;
}) {
  const { interval, refreshVersion } = useAppData();
  const [refreshKey, setRefreshKey] = useState(0);
  const { data, error, loading, lastUpdated } = usePolling<TaskRow>(() => api.task(taskId), interval, [taskId, refreshVersion, refreshKey]);
  const task = data?.task_id === taskId ? data : null;
  const [payload, setPayload] = useState<{ payload?: unknown; _note?: string } | null>(null);
  const [payloadLoading, setPayloadLoading] = useState(false);
  const [confirm, setConfirm] = useState<"delete" | "replay" | "requeue" | null>(null);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [deadlineInput, setDeadlineInput] = useState("");
  const [success, setSuccess] = useState<{ message: string; taskId?: string } | null>(null);
  const mounted = useRef(true);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !event.defaultPrevented && !busy && !confirm) onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose, busy, confirm]);

  const queue = String(task?._queue ?? task?.queue ?? "");
  const state = task ? taskState(task, stateHint) : "unknown";
  const overdue = state === "deadline_missed";
  const replayable = task !== null && (canReplayTask(task) || overdue);
  const deadline = task ? taskDeadline(task) : null;
  const needsDeadline = overdue || (deadline !== null && deadline <= Date.now() / 1000);
  const validDeadline = Boolean(deadlineInput && new Date(deadlineInput).getTime() > Date.now());
  const replayTitle = needsDeadline ? "更新截止并重放" : "重放为新任务";
  const failure = task ? taskError(task) : "";
  const unavailable = busy || Boolean(error);

  const loadPayload = async () => {
    setPayloadLoading(true);
    try {
      const result = await api.taskPayload(taskId, queue, "all");
      if (mounted.current) setPayload(result);
    } catch (cause) {
      if (mounted.current) setPayload({ _note: cause instanceof Error ? cause.message : String(cause) });
    } finally {
      if (mounted.current) setPayloadLoading(false);
    }
  };

  const runAction = async () => {
    if (!task || busy || !confirm) return;
    setBusy(true);
    setActionError(null);
    try {
      if (confirm === "delete") {
        await api.deleteTask(taskId, queue);
        onChanged();
        onClose();
        return;
      }
      if (confirm === "replay") {
        if (!replayable) throw new Error("任务当前状态不支持重放，请刷新详情");
        if (needsDeadline && !validDeadline) throw new Error("请选择晚于当前时间的新执行截止时间");
        const newDeadline = needsDeadline ? new Date(deadlineInput).toISOString() : undefined;
        if (overdue) {
          const result = await api.requeueExpired(queue, newDeadline!, taskId);
          if (result.moved !== 1) throw new Error(result.note ?? "任务未被重放，请刷新后核对状态");
          setSuccess({ message: "已更新截止并生成新任务", taskId: result.new_task_id });
        } else {
          const result = await api.replayTask(taskId, { queue, start_deadline_at: newDeadline });
          if (!result.accepted) throw new Error(result.reason ?? "重放未被接受");
          setSuccess({ message: "已重放为新任务", taskId: result.task_id });
        }
      } else {
        const result = await api.requeueTask(taskId, queue, "dlq");
        setSuccess({ message: result.new_task_id ? "已从死信生成新任务" : "任务已重新入队", taskId: result.new_task_id });
      }
      setConfirm(null);
      onChanged();
    } catch (cause) {
      setActionError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      if (mounted.current) setBusy(false);
    }
  };
  const openConfirm = (kind: "delete" | "replay" | "requeue") => {
    setActionError(null); setSuccess(null); setConfirm(kind);
  };

  return <div className="overlay" onClick={() => { if (!busy) onClose(); }}>
    <div className="drawer" role="dialog" aria-modal="true" aria-label="任务详情" onClick={(event) => event.stopPropagation()}>
      <div className="drawer-head">
        <strong className="mono">{task?.action || "任务"}</strong>
        <span className="mono faint" title={taskId}>{shortId(taskId)}</span>
        <span className="spacer" />
        <StateBadge state={state} label={STATE_LABELS[state as StateKey] ?? "状态未知"} />
        <button className="btn sm" onClick={onClose} disabled={busy} aria-label="关闭">✕</button>
      </div>
      <p className="faint">队列 {queue || "—"} · attempt {task?.attempt ?? "—"}/{task?.max_attempts ?? "—"}
        {task?.logical_key ? ` · logical_key=${task.logical_key}` : ""}</p>
      <div className="table-toolbar">
        <button className="btn sm" disabled={loading || busy} onClick={() => setRefreshKey((key) => key + 1)}>刷新详情</button>
        <DataFreshness lastUpdated={lastUpdated} automatic={interval !== 0} loading={loading} error={error} />
      </div>
      {error && <ErrorBanner error={error} onRetry={() => setRefreshKey((key) => key + 1)} />}
      {!task && !error && <Skeleton lines={8} />}
      {task && <>
        {success && <div className="success-banner" role="status">
          <span>{success.message}</span>
            {success.taskId && <button className="btn sm" disabled={busy} onClick={() => onOpenTask(success.taskId!)}>查看新任务</button>}
        </div>}
        {failure && <section className="task-failure">
          <strong>失败 / 跳过原因</strong>
          {(task.last_error_code || task.reason_code) && <span className="mono faint"> · {String(task.last_error_code || task.reason_code)}</span>}
          <pre className="code">{failure}</pre>
        </section>}
        <Timeline task={task} state={state} />
        <section style={{ margin: "14px 0" }}>
          <div className="drawer-head"><h4>Payload / Result</h4><span className="spacer" />
            <button className="btn sm" onClick={loadPayload} disabled={payloadLoading}>
              {payloadLoading ? "拉取中…" : payload ? "重新加载 payload" : "加载完整 payload"}
            </button>
          </div>
          {!payload && <p className="faint">按需还原完整业务参数，大 payload 可能需要稍等。</p>}
          {payload && <>
            {payload._note && <p className="muted">{payload._note}</p>}
            <pre className="code">{JSON.stringify(payload.payload ?? null, null, 2)}</pre>
          </>}
          {task.result !== undefined && task.result !== null ? <>
            <h4>Result</h4><pre className="code">{JSON.stringify(task.result, null, 2)}</pre>
          </> : <p className="faint">任务尚无 result 记录。</p>}
        </section>
        {(task.replay_of || task.replayed_by) && <section className="example-box">
          <div className="faint">任务血缘</div>
          {task.replay_of && <p>来源任务 <button className="text-link mono" disabled={busy} title={String(task.replay_of)}
            onClick={() => onOpenTask(String(task.replay_of))}>{shortId(String(task.replay_of))}</button></p>}
          {replayIds(task.replayed_by).map((id) => <p key={id}>重放任务 <button className="text-link mono" disabled={busy} title={id}
            onClick={() => onOpenTask(id)}>{shortId(id)}</button></p>)}
        </section>}
        {state === "processing" && <p className="example-box">任务正在执行，等待完成后可进行后续操作。失联任务可在 Worker 页恢复。</p>}
        <div className="drawer-actions">
          {replayable && <button className="btn primary" disabled={unavailable} onClick={() => openConfirm("replay")}>{replayTitle}</button>}
          {state === "dlq" && !canReplayTask(task) && <button className="btn" disabled={unavailable}
            onClick={() => openConfirm("requeue")}>从 DLQ 重入队</button>}
          {DELETABLE.has(state) && <button className="btn danger" disabled={unavailable} onClick={() => openConfirm("delete")}>删除</button>}
        </div>
      </>}
      <ConfirmDialog open={confirm !== null} title={confirm === "delete" ? "删除任务" : confirm === "requeue" ? "从 DLQ 重入队" : replayTitle}
        danger={confirm === "delete"} confirmText={confirm === "delete" ? "删除" : confirm === "requeue" ? "重入队" : "重放"}
        busy={busy} error={actionError ?? error} confirmDisabled={Boolean(error) || (confirm === "replay" && !replayable)}
        body={confirm === "delete" ? <p>将删除该任务的消息与历史记录，不可恢复。</p> : confirm === "requeue" ?
          <p>该死信任务将回到排队状态，重试计数重置。</p> : <>
            <p>{overdue ? "将取消这条已过期的待处理任务，保留审计记录并生成新任务。" : "使用原业务参数生成新任务并保留血缘，原终态记录保留。"}</p>
            {needsDeadline && <label>新的执行截止时间
              <input className="input" type="datetime-local" aria-label="新的执行截止时间" value={deadlineInput}
                disabled={busy} onChange={(event) => { setDeadlineInput(event.target.value); setActionError(null); }} />
              <p className="faint">原截止时间已过，请选择晚于当前时间的新截止时间。</p>
            </label>}
          </>}
        onCancel={() => { setConfirm(null); setActionError(null); }} onConfirm={runAction} />
    </div>
  </div>;
}

function Timeline({ task, state }: { task: TaskRow; state: string }) {
  const events = [{ time: fmtTime(task.created_at), label: "投递", color: "var(--c-primary)" }];
  if (task.last_started_at || task.started_at) events.push({ time: fmtTime(task.last_started_at ?? task.started_at),
    label: `最近一次开始执行 attempt=${task.attempt ?? "—"}`, color: "var(--c-primary)" });
  if (state === "retry_wait" || state === "delay") events.push({
    time: fmtTime((task.available_at ?? task.run_at) as number | string | undefined),
    label: state === "retry_wait" ? "等待自动重试" : task.delay_reason === "concurrency" ? "等待并发条件" : "计划延迟执行",
    color: "var(--c-warning)",
  });
  if ((TERMINAL.has(state) || state === "dlq") && task.finished_at) events.push({ time: fmtTime(task.finished_at),
    label: STATE_LABELS[state as StateKey] ?? state, color: state === "completed" ? "var(--c-success)" : "var(--c-danger)" });
  else if (task.updated_at) events.push({ time: fmtTime(task.updated_at), label: "记录更新时间", color: "var(--c-muted)" });
  return <section className="card" style={{ padding: 12 }}><div className="faint">时间线</div>
    <ul className="timeline">{events.map((event, index) => <li key={index}>
      <span className="t-dot" style={{ background: event.color }} /><span className="t-time">{event.time}</span>
      <span>{event.label}</span>
    </li>)}</ul>
  </section>;
}
