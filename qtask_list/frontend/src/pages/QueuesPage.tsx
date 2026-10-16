import { useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { api } from "../api";
import { useAppData } from "../appData";
import { fmtInt, fmtRate } from "../sampler";
import { ConfirmDialog, EmptyState, ErrorBanner, OpsMenu, QueueComposition, Skeleton, StateBadge } from "../components/ui";
import { PushTaskDialog } from "../components/PushTaskDialog";
import type { QueueInfo } from "../types";
import { useStoredPreference } from "../preferences";

type FilterKey = "all" | "active" | "abnormal";

export function QueuesPage() {
  const navigate = useNavigate();
  const { queues, progress: queueProgress, error, loading, reload } = useAppData();
  const [params] = useSearchParams();
  const [nsFilter, setNsFilter] = useStoredPreference("qtask.queues.namespace.v1", "全部", (value): value is string => typeof value === "string");
  const [nameSearch, setNameSearch] = useStoredPreference("qtask.queues.search.v1", "", (value): value is string => typeof value === "string");
  const filter = (params.get("filter") as FilterKey) || "all";

  const [confirm, setConfirm] = useState<{ kind: "clear" | "delete" | "recover" | "requeueDlq" | "retry"; queue: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [opError, setOpError] = useState<string | null>(null);
  const [pushQueue, setPushQueue] = useState<string | null>(null);

  const namespaces = useMemo(() => {
    const set = new Set<string>();
    for (const q of queues ?? []) {
      const name = String(q.name);
      set.add(name.includes(":") ? name.slice(0, name.indexOf(":")) : "(默认)");
    }
    return ["全部", ...[...set].sort()];
  }, [queues]);

  const cards = useMemo(() => {
    if (!queues) return [];
    const list = queues.map((item: QueueInfo) => {
      const stats = item as unknown as Record<string, number>;
      const name = String(item.name);
      const progress = queueProgress[name];
      const noWorker = (stats.queue ?? 0) > 0 && (stats.active_workers ?? 0) === 0;
      const scheduledOnly = (stats.queue ?? 0) === 0 && (stats.processing ?? 0) === 0 &&
        (stats.retry ?? 0) === 0 && (stats.delay ?? 0) > 0;
      const abnormal =
        (stats.dlq ?? 0) > 0 || progress.stalled || noWorker || (stats.stale_workers ?? 0) > 0 || (stats.deadline_missed ?? 0) > 0;
      return { name, stats, progress, abnormal, noWorker, scheduledOnly };
    });
    let out = list;
    if (filter === "active") out = out.filter((c) => c.progress.remaining > 0);
    if (filter === "abnormal") out = out.filter((c) => c.abnormal);
    if (nsFilter !== "全部") {
      out = out.filter((c) => (nsFilter === "(默认)" ? !c.name.includes(":") : c.name.startsWith(`${nsFilter}:`)));
    }
    if (nameSearch) out = out.filter((c) => c.name.toLowerCase().includes(nameSearch.toLowerCase()));
    return out.sort((a, b) => (a.abnormal === b.abnormal ? b.progress.remaining - a.progress.remaining : a.abnormal ? -1 : 1));
  }, [queues, filter, nsFilter, nameSearch, queueProgress]);

  const runOp = async (fn: () => Promise<unknown>) => {
    if (busy) return;
    setBusy(true);
    setOpError(null);
    try {
      await fn();
      setConfirm(null);
      reload();
    } catch (e) {
      setOpError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="page">
      <h1 className="page-title">队列</h1>
      <p className="page-desc">按 namespace 管理队列。色条显示当前排队、执行、延迟与旧重试构成；速率与 ETA 为浏览器观测估计。</p>
      {error && <ErrorBanner error={error} onRetry={reload} />}
      {opError && !confirm && <ErrorBanner error={opError} onRetry={() => setOpError(null)} />}

      {loading && !queues ? (
        <Skeleton lines={6} height={40} />
      ) : !queues ? (
        <EmptyState icon="⚠" title="队列数据不可用">连接恢复后点击顶栏“刷新”。</EmptyState>
      ) : queues && queues.length === 0 ? (
        <EmptyState icon="📭" title="没有队列，去投递第一条任务">
          用 <code>qtask push</code> 投递，或启动你的 Worker 后回来看这里。
        </EmptyState>
      ) : (
        <>
          <div className="filter-bar">
            <select className="input" value={nsFilter} onChange={(e) => setNsFilter(e.target.value)}>
              {namespaces.map((ns) => (
                <option key={ns}>{ns}</option>
              ))}
            </select>
            <div className="seg">
              {(
                [
                  ["all", "全部"],
                  ["active", "进行中"],
                  ["abnormal", "仅异常"],
                ] as Array<[FilterKey, string]>
              ).map(([key, label]) => (
                <button key={key} className={filter === key ? "on" : ""} onClick={() => navigate(`/queues${key === "all" ? "" : `?filter=${key}`}`)}>
                  {label}
                </button>
              ))}
            </div>
            <input className="input" placeholder="搜索队列名" value={nameSearch} onChange={(e) => setNameSearch(e.target.value)} />
            <span className="faint">共 {cards.length} 个</span>
          </div>

          <div className="queue-grid">
            {cards.map((c) => (
              <div className="card queue-card" key={c.name}>
                <div className="qc-head">
                  <span className="qc-name" title={c.name}>{c.name}</span>
                  <span style={{ flex: 1 }} />
                  {c.abnormal && <StateBadge state="failed" label="异常" />}
                </div>
                <QueueComposition stats={c.stats} />
                <div className="qc-stats num">
                  待处理 {fmtInt(c.progress.remaining)} · 观测期完成 {fmtInt(c.progress.completedObserved)} · 速率 {fmtRate(c.progress.ratePerMin)} ·{" "}
                  {c.noWorker ? <span style={{ color: "var(--c-danger)" }}>无 Worker</span> : c.progress.stalled ? <span style={{ color: "var(--c-danger)" }}>疑似停滞 ⚠</span> : c.scheduledOnly ? c.stats.retry_wait > 0 ? "等待重试" : "等待调度" : c.progress.etaText ?? "ETA —"}
                  {(c.stats.dlq ?? 0) > 0 && <span style={{ color: "var(--c-danger)" }}> · DLQ {c.stats.dlq}</span>}
                </div>
                <div className="qc-actions">
                  <button className="btn sm" onClick={() => navigate(`/queues/${encodeURIComponent(c.name)}`)}>
                    详情
                  </button>
                  <OpsMenu
                    label="操作"
                    items={[
                      { label: "投递任务…", onClick: () => setPushQueue(c.name) },
                      { label: "重放全部 DLQ", onClick: () => setConfirm({ kind: "requeueDlq", queue: c.name }) },
                      { label: "恢复失联 processing", onClick: () => setConfirm({ kind: "recover", queue: c.name }) },
                      { label: "drain 手动重试", onClick: () => setConfirm({ kind: "retry", queue: c.name }) },
                      { label: "清空队列", danger: true, onClick: () => setConfirm({ kind: "clear", queue: c.name }) },
                      { label: "删除队列", danger: true, onClick: () => setConfirm({ kind: "delete", queue: c.name }) },
                    ]}
                  />
                </div>
              </div>
            ))}
          </div>
          {cards.length === 0 && <EmptyState title="当前筛选下没有队列">
            <button className="btn" onClick={() => { setNsFilter("全部"); setNameSearch(""); navigate("/queues"); }}>清空筛选</button>
          </EmptyState>}
        </>
      )}

      {pushQueue && (
        <PushTaskDialog
          queue={pushQueue}
          open
          onClose={() => setPushQueue(null)}
          onPushed={reload}
        />
      )}
      <ConfirmDialog
        open={confirm?.kind === "clear"}
        error={opError}
        title={`清空队列 ${confirm?.queue ?? ""}`}
        danger
        confirmKeyword={confirm?.queue}
        confirmText="清空"
        busy={busy}
        body={<p>将清空 ready/retry/delay/dlq 中所有消息（默认含 DLQ，不含历史）。identity 保留。此操作不可恢复。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => confirm && runOp(() => api.clearQueue(confirm.queue, true, false, false))}
      />
      <ConfirmDialog
        open={confirm?.kind === "delete"}
        error={opError}
        title={`删除队列 ${confirm?.queue ?? ""}`}
        danger
        confirmKeyword={confirm?.queue}
        confirmText="删除"
        busy={busy}
        body={<p>将删除该队列全部数据（含历史索引）。请确认没有 Worker 正在使用。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => confirm && runOp(() => api.deleteQueue(confirm.queue))}
      />
      <ConfirmDialog
        open={confirm?.kind === "recover"}
        error={opError}
        title={`恢复失联 processing ${confirm?.queue ?? ""}`}
        confirmText="恢复"
        busy={busy}
        body={<p>将把失联 Worker processing 中的任务移回主队列。活跃 Worker 不受影响。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => confirm && runOp(() => api.recoverQueue(confirm.queue, false))}
      />
      <ConfirmDialog
        open={confirm?.kind === "requeueDlq"}
        error={opError}
        title={`重放全部 DLQ ${confirm?.queue ?? ""}`}
        confirmText="重放"
        busy={busy}
        body={<p>DLQ 中所有任务将重置重试计数并回到 ready。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => confirm && runOp(() => api.requeueDlq(confirm.queue, null))}
      />
      <ConfirmDialog
        open={confirm?.kind === "retry"}
        error={opError}
        title={`drain 手动重试 ${confirm?.queue ?? ""}`}
        confirmText="执行"
        busy={busy}
        body={<p>retry 队列中的任务将移回 ready。</p>}
        onCancel={() => setConfirm(null)}
        onConfirm={() => confirm && runOp(() => api.retryQueue(confirm.queue))}
      />
    </div>
  );
}
