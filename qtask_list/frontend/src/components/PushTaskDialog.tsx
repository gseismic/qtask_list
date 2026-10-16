import { useState } from "react";
import { api, type PushTaskBody } from "../api";

export function PushTaskDialog({
  queue,
  open,
  onClose,
  onPushed,
}: {
  queue: string;
  open: boolean;
  onClose: () => void;
  onPushed: () => void;
}) {
  const [action, setAction] = useState("");
  const [payloadText, setPayloadText] = useState("{}");
  const [delay, setDelay] = useState("0");
  const [expire, setExpire] = useState("0");
  const [logicalKey, setLogicalKey] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  if (!open) return null;

  const submit = async () => {
    let payload: Record<string, unknown>;
    try {
      const parsed: unknown = JSON.parse(payloadText || "{}");
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        setError("payload 必须是 JSON 对象");
        return;
      }
      payload = parsed as Record<string, unknown>;
    } catch {
      setError("payload 不是合法 JSON");
      return;
    }
    const selectedAction = action.trim() || payload.action;
    if (typeof selectedAction !== "string" || !selectedAction.trim()) {
      setError("请填写已注册的 handler action，或在 payload 中提供 action");
      return;
    }
    const delaySeconds = Number(delay || "0");
    const expireSeconds = Number(expire || "0");
    if (![delaySeconds, expireSeconds].every((n) => Number.isInteger(n) && n >= 0)) {
      setError("延迟和截止秒数必须是非负整数");
      return;
    }
    const body: PushTaskBody = {
      payload,
      action: action.trim() || null,
      delay_seconds: delaySeconds,
      expire_seconds: expireSeconds,
      logical_key: logicalKey || null,
    };
    setBusy(true);
    setError(null);
    try {
      const result = await api.pushTask(queue, body);
      if (!result.accepted) {
        const reasons: Record<string, string> = {
          duplicate_active: "相同 logical_key 的任务仍在等待或执行",
          duplicate_retained: "相同 logical_key 的任务仍在去重保留期",
        };
        setError(`未投递：${reasons[result.reason] ?? result.reason ?? "请求未被接受"}${result.duplicate_of ? `；已有任务 ${result.duplicate_of}` : ""}`);
        return;
      }
      onPushed();
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="dialog-backdrop" onClick={() => { if (!busy) onClose(); }}>
      <div className="dialog" role="dialog" aria-modal="true" aria-label="投递任务" onClick={(e) => e.stopPropagation()}>
        <h3 style={{ marginTop: 0 }}>投递任务 → {queue}</h3>
        <p className="faint">任务会进入真实队列，由在线 Worker 执行，并可能产生业务副作用。</p>
        <div style={{ display: "grid", gap: 10 }}>
          <label className="faint">
            handler action（或写在 payload.action 中）
            <input className="input" style={{ width: "100%" }} value={action} onChange={(e) => setAction(e.target.value)} placeholder="如 fetch_quote" />
          </label>
          <label className="faint">
            payload（JSON）
            <textarea className="input mono" rows={6} style={{ width: "100%" }} value={payloadText} onChange={(e) => setPayloadText(e.target.value)} />
          </label>
          <div style={{ display: "flex", gap: 10 }}>
            <label className="faint" style={{ flex: 1 }}>
              延迟秒数
              <input className="input" type="number" min={0} style={{ width: "100%" }} value={delay} onChange={(e) => setDelay(e.target.value)} />
            </label>
            <label className="faint" style={{ flex: 1 }}>
              截止秒数（expire）
              <input className="input" type="number" min={0} style={{ width: "100%" }} value={expire} onChange={(e) => setExpire(e.target.value)} />
            </label>
          </div>
          <label className="faint">
            logical_key（可选，幂等去重）
            <input className="input mono" style={{ width: "100%" }} value={logicalKey} onChange={(e) => setLogicalKey(e.target.value)} placeholder="如 quote:AAPL:20260921T1000" />
          </label>
          {error && <div className="error-banner" style={{ marginBottom: 0 }}>⚠ {error}</div>}
        </div>
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 16 }}>
          <button className="btn" onClick={onClose} disabled={busy}>
            取消
          </button>
          <button className="btn primary" onClick={submit} disabled={busy}>
            {busy ? "投递中…" : "投递"}
          </button>
        </div>
      </div>
    </div>
  );
}
