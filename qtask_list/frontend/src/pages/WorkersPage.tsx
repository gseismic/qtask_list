import { useState } from "react";
import { api } from "../api";
import { useAppData } from "../appData";
import { ConfirmDialog, ErrorBanner, KpiCard, Skeleton, StateBadge, fmtTime } from "../components/ui";

export function WorkersPage() {
  const { workers, health, error, loading, reload } = useAppData();
  const [confirm, setConfirm] = useState<{ queue: string; workerId: string; count: number } | null>(null);
  const [busy, setBusy] = useState(false);
  const [opError, setOpError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const online = (workers ?? []).filter((w) => w.active);
  const lost = (workers ?? []).filter((w) => !w.active);
  const totalProcessing = (workers ?? []).reduce((a, w) => a + w.processing, 0);
  const mem = health?.memory;
  const memPct =
    mem?.used_memory && mem?.maxmemory && mem.maxmemory > 0 ? Math.round((mem.used_memory / mem.maxmemory) * 100) : null;
  const memDanger = mem?.status === "warning";

  const recover = async (queue: string, workerId: string) => {
    if (busy) return;
    setBusy(true);
    setOpError(null);
    try {
      const result = await api.recoverQueue(queue, false, workerId);
      setNotice(`Worker ${workerId}：已恢复 ${result.recovered} 条任务，跳过活跃任务 ${result.skipped_active} 条。`);
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
      <h1 className="page-title">Worker 监控</h1>
      <p className="page-desc">Worker 心跳与失联恢复；Redis 内存水位。</p>
      {error && <ErrorBanner error={error} onRetry={reload} />}
      {opError && !confirm && <ErrorBanner error={opError} />}
      {notice && <div className="success-banner" role="status">{notice}</div>}

      {workers && <div className="kpi-row">
        <KpiCard label="在线 Worker" value={online.length} />
        <KpiCard label="失联 Worker" value={lost.length} danger={lost.length > 0} />
        <KpiCard label="processing 总数" value={totalProcessing} />
        <KpiCard
          label="Redis 内存"
          value={memPct !== null ? `${memPct}%` : (mem?.used_memory_human ?? "—")}
          danger={memDanger}
          hint={mem?.maxmemory_human ? `峰值 ${mem.used_memory_peak_human ?? "—"} · 上限 ${mem.maxmemory_human}` : "未设置 maxmemory"}
        />
      </div>}

      {loading && !workers ? (
        <Skeleton lines={6} height={20} />
      ) : !workers ? (
        <div className="empty">Worker 数据不可用，请检查 Redis 连接后刷新。</div>
      ) : workers && workers.length === 0 ? (
        <div className="empty">
          <div className="big">🧑‍🏭</div>
          <div style={{ fontWeight: 600 }}>没有在线 Worker</div>
          <p className="muted">参考 README 启动 Worker，例如：</p>
          <code>qtask worker --module your_worker_module:worker</code>
          <p className="faint">确认后这里会出现心跳。</p>
        </div>
      ) : (
        <div className="table-wrap">
          <table className="tbl">
            <thead>
              <tr>
                <th>worker_id</th>
                <th>队列</th>
                <th>processing</th>
                <th>心跳</th>
                <th>状态</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {(workers ?? []).map((w) => (
                <tr key={`${w.queue}:${w.worker_id}`}>
                  <td className="mono">{w.worker_id}</td>
                  <td className="mono">{w.queue}</td>
                  <td className="num">{w.processing}</td>
                  <td className="faint num">
                    {w.active ? `${w.ttl}s 后过期` : w.last_seen ? fmtTime(w.last_seen) : "无心跳"}
                  </td>
                  <td>
                    <StateBadge state={w.active ? "processing" : "failed"} label={w.active ? "在线" : "失联"} />
                  </td>
                  <td style={{ textAlign: "right" }}>
                    {!w.active && w.processing > 0 && (
                      <button
                        className="btn sm"
                        onClick={() => setConfirm({ queue: w.queue, workerId: w.worker_id, count: w.processing })}
                      >
                        恢复
                      </button>
                    )}
                    {!w.active && w.processing === 0 && <span className="faint">无遗留任务</span>}
                    {w.active && <span className="faint">运行中</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {health && <div className="card" style={{ marginTop: 20 }}>
        <h2 className="card-title">Redis 内存</h2>
        {memPct !== null && (
          <div className="pbar" style={{ height: 12, marginBottom: 10 }} title={`${memPct}%`}>
            <div
              className="seg-remaining"
              style={{
                width: `${memPct}%`,
                background: memDanger ? "var(--c-danger)" : "var(--c-primary)",
              }}
            />
          </div>
        )}
        <div className="kv">
          <dt>已用</dt>
          <dd className="num">{mem?.used_memory_human ?? "—"}</dd>
          <dt>峰值</dt>
          <dd className="num">{mem?.used_memory_peak_human ?? "—"}</dd>
          <dt>上限 maxmemory</dt>
          <dd className="num">{mem?.maxmemory_human ?? "未设置"}</dd>
          <dt>状态</dt>
          <dd>{health.status === "error" ? <span className="badge c-danger">不可用</span> : memDanger ? <span className="badge c-danger">超过告警阈值</span> : <span className="badge c-success">正常</span>}</dd>
        </div>
      </div>}

      <ConfirmDialog
        open={confirm !== null}
        error={opError}
        title={`恢复失联 Worker ${confirm?.workerId ?? ""}`}
        confirmText="恢复"
        busy={busy}
        body={
          <p>
            将把该 Worker processing 中的 <b>{confirm?.count ?? 0}</b> 个任务移回主队列（队列 {confirm?.queue}）。
          </p>
        }
        onCancel={() => { setConfirm(null); setOpError(null); }}
        onConfirm={() => confirm && recover(confirm.queue, confirm.workerId)}
      />
    </div>
  );
}
