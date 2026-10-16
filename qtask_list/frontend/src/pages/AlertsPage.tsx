import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useAppData } from "../appData";
import { ALERT_RULES_DOC, loadResolved } from "../alerts";
import { EmptyState, ErrorBanner, Skeleton } from "../components/ui";
import { fmtTime } from "../components/ui";

export function AlertsPage() {
  const { queues, alerts, error, loading, reload, resolveAlert, clearResolved } = useAppData();
  const [tab, setTab] = useState<"open" | "resolved">("open");
  const [showRules, setShowRules] = useState(false);
  const [writeError, setWriteError] = useState<string | null>(null);

  const resolvedMap = useMemo(() => {
    return loadResolved();
  }, [alerts]);

  const resolvedKeys = Object.keys(resolvedMap);

  return (
    <div className="page">
      <h1 className="page-title">告警中心</h1>
      <p className="page-desc">DLQ、无人消费、失败率、失联 Worker、积压、过期与 Redis 内存的聚合信号。</p>
      {error && <ErrorBanner error={error} onRetry={reload} />}
      {writeError && <ErrorBanner error={writeError} />}

      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
        <div className="seg">
          <button className={tab === "open" ? "on" : ""} onClick={() => setTab("open")}>
            未处理 ({queues ? alerts.length : "—"})
          </button>
          <button className={tab === "resolved" ? "on" : ""} onClick={() => setTab("resolved")}>
            已处理 ({resolvedKeys.length})
          </button>
        </div>
        <span style={{ flex: 1 }} />
        <button className="btn sm" onClick={() => setShowRules((v) => !v)}>
          规则说明 ⓘ
        </button>
      </div>

      {showRules && (
        <div className="card" style={{ marginBottom: 16 }}>
          <table className="ops-table">
            <thead>
              <tr>
                <th>规则</th>
                <th>条件</th>
                <th>严重度</th>
                <th>数据源</th>
                <th>说明</th>
              </tr>
            </thead>
            <tbody>
              {ALERT_RULES_DOC.map((r) => (
                <tr key={r.name}>
                  <td>{r.name}</td>
                  <td className="mono">{r.rule}</td>
                  <td>
                    <span className={`badge ${r.severity === "danger" ? "c-danger" : "c-warning"}`}>{r.severity}</span>
                  </td>
                  <td className="faint">{r.source}</td>
                  <td className="muted">{r.note}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="faint">阈值为初始假设，待实际运行校准；"标记已处理"仅存本浏览器，同规则复发会重新出现。</p>
        </div>
      )}

      {loading && !queues ? (
        <Skeleton lines={5} height={30} />
      ) : !queues ? (
        <EmptyState icon="⚠" title="告警状态不可判定">监控数据不可用，连接恢复后请刷新。</EmptyState>
      ) : tab === "open" ? (
        alerts.length === 0 ? (
          <EmptyState icon={error ? "⚠" : "🎉"} title={error ? "告警数据已过时" : "当前没有告警"}>
            <p className="muted">
              {error ? "最近一次刷新失败，不能据此判断当前是否有告警。" : "告警规则：DLQ>0、ready 有任务但无在线 Worker、累计失败率>5%、stale worker、ready>10000 持续 10 分钟、任务过期、Worker 心跳超时、Redis 内存超限。"}
            </p>
          </EmptyState>
        ) : (
          <div style={{ display: "grid", gap: 8 }}>
            {alerts.map((a) => (
              <div key={a.key} className="card" style={{ padding: 12, borderColor: a.severity === "danger" ? "var(--danger-border)" : undefined }}>
                <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
                  <span className={`badge ${a.severity === "danger" ? "c-danger" : "c-warning"}`}>
                    <span className="dot" style={{ background: "currentColor" }} />
                    {a.ruleLabel}
                  </span>
                  <b>{a.target}</b>
                  <span style={{ flex: 1 }}>{a.detail}</span>
                  <span className="faint num">{fmtTime(a.firstSeen / 1000)}</span>
                  {a.locate.page === "queue" && a.locate.queue && (
                    <Link className="btn sm" to={`/queues/${encodeURIComponent(a.locate.queue)}?state=${a.locate.state ?? "all"}`}>
                      定位
                    </Link>
                  )}
                  {a.locate.page === "workers" && (
                    <Link className="btn sm" to="/workers">
                      查看 Worker
                    </Link>
                  )}
                  <button
                    className="btn sm"
                    onClick={() => {
                      try { resolveAlert(a.key); setWriteError(null); }
                      catch { setWriteError("无法保存告警确认，请检查浏览器存储设置后重试该操作。"); }
                    }}
                  >
                    标记已处理
                  </button>
                </div>
              </div>
            ))}
          </div>
        )
      ) : resolvedKeys.length === 0 ? (
        <EmptyState icon="🗂" title="还没有已处理记录" />
      ) : (
        <div style={{ display: "grid", gap: 8 }}>
          {resolvedKeys.map((key) => (
            <div key={key} className="card" style={{ padding: 12 }}>
              <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                <span className="badge c-muted">已处理</span>
                <code className="mono">{key}</code>
                <span style={{ flex: 1 }} />
                <span className="faint num">{fmtTime(resolvedMap[key] / 1000)}</span>
              </div>
            </div>
          ))}
          <div>
            <button
              className="btn sm"
              onClick={() => {
                try { clearResolved(); setWriteError(null); }
                catch { setWriteError("无法清空已处理记录，请检查浏览器存储设置后重试该操作。"); }
              }}
            >
              清空已处理记录
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
