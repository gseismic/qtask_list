import { useEffect, useId, useRef, useState } from "react";
import type { StateKey } from "../types";
import { useModalFocus } from "../modal";

export function fmtTime(v: number | string | null | undefined): string {
  if (v === null || v === undefined || v === "") return "—";
  const n = typeof v === "string" ? Number(v) : v;
  const d = Number.isFinite(n) ? new Date(n * 1000) : new Date(v as string);
  if (Number.isNaN(d.getTime())) return String(v);
  const pad = (x: number) => String(x).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

export function shortId(id: string | undefined): string {
  if (!id) return "—";
  return id.length > 12 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id;
}

const STATE_COLOR: Record<string, string> = {
  ready: "c-primary",
  queue: "c-primary",
  processing: "c-primary",
  retry: "c-warning",
  retry_wait: "c-warning",
  delay: "c-warning",
  completed: "c-success",
  failed: "c-danger",
  dlq: "c-danger",
  deadline_missed: "c-danger",
  expired: "c-danger",
  skipped: "c-muted",
  cancelled: "c-muted",
  history: "c-muted",
};

export function StateBadge({ state, label }: { state: string; label?: string }) {
  const color = STATE_COLOR[state] ?? "c-muted";
  const breathe = state === "processing" ? " breathe" : "";
  return (
    <span className={`badge ${color}`}>
      <span className={`dot${breathe}`} style={{ background: "currentColor" }} />
      {label ?? state}
    </span>
  );
}

export function KpiCard({
  label,
  value,
  hint,
  actionHint,
  danger,
  onClick,
  title,
}: {
  label: string;
  value: React.ReactNode;
  hint?: string;
  actionHint?: string;
  danger?: boolean;
  onClick?: () => void;
  title?: string;
}) {
  const cls = `kpi${danger ? " danger" : ""}${onClick ? " clickable" : ""}`;
  const accessibleValue = typeof value === "string" || typeof value === "number" ? value : "";
  if (onClick) {
    return (
      <button type="button" className={cls} onClick={onClick} title={title ?? actionHint}
        aria-label={actionHint ? `${label} ${accessibleValue}. ${actionHint}` : undefined}>
        <div className="kpi-label">{label}</div>
        <div className="kpi-value">{value}</div>
        {hint && <div className="kpi-hint">{hint}</div>}
        {actionHint && <div className="kpi-action-hint" aria-hidden="true">{actionHint}</div>}
      </button>
    );
  }
  return (
    <div className={cls} title={title}>
      <div className="kpi-label">{label}</div>
      <div className="kpi-value">{value}</div>
      {hint && <div className="kpi-hint">{hint}</div>}
    </div>
  );
}

export function QueueComposition({ stats }: { stats: Record<string, number> }) {
  const segments = [
    ["ready", "排队", stats.queue ?? 0],
    ["processing", "执行", stats.processing ?? 0],
    ["delay", "延迟", stats.delay ?? 0],
    ["retry", "旧重试", stats.retry ?? 0],
  ] as const;
  const total = segments.reduce((sum, [, , count]) => sum + count, 0);
  const description = segments.map(([, label, count]) => `${label} ${count}`).join(" · ");
  return <div className="pbar" role="img" aria-label={total > 0 ? description : "无待处理任务"} title={description}>
    {segments.map(([key, , count]) => count > 0 && <div
      key={key} className={`seg-${key}`} style={{ flex: count, minWidth: 2 }}
    />)}
  </div>;
}

export function DataFreshness({ lastUpdated, automatic, loading, error }: {
  lastUpdated: number | null; automatic: boolean; loading: boolean; error: string | null;
}) {
  return <span className="faint" role="status">
    {error ? "数据已过时" : loading ? "更新中…" : automatic ? "自动刷新" : "查询快照 · 自动刷新已暂停"}
    {lastUpdated && ` · 更新于 ${new Date(lastUpdated).toLocaleTimeString("zh-CN")}`}
  </span>;
}

export function ErrorBanner({ error, onRetry }: { error: string; onRetry?: () => void }) {
  return (
    <div className="error-banner" role="alert">
      <span>⚠ {error}</span>
      <span className="spacer" style={{ flex: 1 }} />
      {onRetry && (
        <button className="btn sm" onClick={onRetry}>
          重试
        </button>
      )}
    </div>
  );
}

export function Skeleton({ lines = 5, height = 14 }: { lines?: number; height?: number }) {
  return (
    <div style={{ display: "grid", gap: 10 }}>
      {Array.from({ length: lines }, (_, i) => (
        <div key={i} className="skel" style={{ height }} />
      ))}
    </div>
  );
}

export function EmptyState({ icon, title, children }: { icon?: string; title: string; children?: React.ReactNode }) {
  return (
    <div className="empty">
      {icon && <div className="big">{icon}</div>}
      <div style={{ fontWeight: 600, marginBottom: 4 }}>{title}</div>
      {children}
    </div>
  );
}

export function ConfirmDialog({
  open,
  title,
  danger,
  confirmText,
  confirmKeyword,
  body,
  busy,
  error,
  confirmDisabled,
  onConfirm,
  onCancel,
}: {
  open: boolean;
  title: string;
  danger?: boolean;
  confirmText: string;
  confirmKeyword?: string;
  body: React.ReactNode;
  busy?: boolean;
  error?: string | null;
  confirmDisabled?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const [typed, setTyped] = useState("");
  const dialogRef = useRef<HTMLDivElement>(null);
  const titleId = useId();
  useModalFocus(dialogRef, { open, busy, onClose: onCancel });
  useEffect(() => {
    if (open) setTyped("");
  }, [open]);
  if (!open) return null;
  const needKeyword = !!confirmKeyword;
  const ok = !needKeyword || typed === confirmKeyword;
  return (
    <div className="dialog-backdrop" onClick={() => { if (!busy) onCancel(); }}>
      <div ref={dialogRef} role="dialog" aria-modal="true" aria-labelledby={titleId} tabIndex={-1}
        className={`dialog${danger ? " danger" : ""}`} onClick={(e) => e.stopPropagation()}>
        <h3 id={titleId} style={{ marginTop: 0 }}>{title}</h3>
        <div className="muted" style={{ marginBottom: 14 }}>
          {body}
        </div>
        {needKeyword && (
          <div style={{ marginBottom: 14 }}>
            <div className="faint" style={{ marginBottom: 4 }}>
              输入 <code>{confirmKeyword}</code> 确认：
            </div>
            <input className="input" style={{ width: "100%" }} value={typed} disabled={busy} onChange={(e) => setTyped(e.target.value)} />
          </div>
        )}
        {error && <div className="error-banner" role="alert">{error}</div>}
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button className="btn" onClick={onCancel} disabled={busy}>
            取消
          </button>
          <button
            className={`btn ${danger ? "danger" : "primary"}`}
            onClick={onConfirm}
            disabled={!ok || busy || confirmDisabled}
          >
            {busy ? "执行中…" : confirmText}
          </button>
        </div>
      </div>
    </div>
  );
}

export function OpsMenu({ label, items }: { label: string; items: Array<{ label: string; danger?: boolean; onClick: () => void; hidden?: boolean }> }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, [open]);
  const visible = items.filter((i) => !i.hidden);
  if (visible.length === 0) return null;
  const dangerStart = visible.findIndex((i) => i.danger);
  return (
    <div className="menu-wrap" ref={ref}>
      <button className="btn" onClick={() => setOpen((o) => !o)}>
        {label} ▾
      </button>
      {open && (
        <div className="menu">
          {visible.map((item, idx) => (
            <div key={item.label}>
              {idx === dangerStart && dangerStart > 0 && <div className="menu-sep" />}
              <button
                className={item.danger ? "danger" : ""}
              onClick={() => {
                  setOpen(false);
                  ref.current?.querySelector<HTMLButtonElement>("button")?.focus();
                  item.onClick();
                }}
              >
                {item.label}
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export function stateTabLabel(state: StateKey): { label: string; hot: boolean } {
  const hot = state === "dlq" || state === "deadline_missed" || state === "failed";
  return { label: state, hot };
}
