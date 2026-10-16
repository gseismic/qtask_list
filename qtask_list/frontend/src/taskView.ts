import type { TaskRow } from "./types";

const TERMINAL = new Set(["completed", "failed", "skipped", "cancelled"]);
const REPLAYABLE = new Set(["failed", "skipped", "cancelled"]);
const KNOWN = new Set(["ready", "processing", "retry", "retry_wait", "delay", "dlq", ...TERMINAL]);

export function taskDeadline(task: TaskRow): number | null {
  const value = Number(task.start_deadline_at || task.expires_at);
  return Number.isFinite(value) && value > 0 ? value : null;
}

/** 最新生命周期与容器决定状态，列表提示仅兼容缺字段的旧记录。 */
export function taskState(task: TaskRow, hint = "", now = Date.now() / 1000): string {
  const outcome = String(task.outcome || task.status || "");
  const location = String(task.location || "");
  if (TERMINAL.has(outcome)) return outcome === "failed" && location === "dlq" ? "dlq" : outcome;
  if (location === "processing") return "processing";
  const deadline = taskDeadline(task);
  if (deadline !== null && deadline <= now && ["ready", "retry", "delay"].includes(location)) return "deadline_missed";
  if (location === "delay") return task.delay_reason === "retry" ? "retry_wait" : "delay";
  if (KNOWN.has(location)) return location;
  const legacyState = String(task._state || task.state || task.status || "");
  if (KNOWN.has(legacyState)) return legacyState;
  if (KNOWN.has(hint) || hint === "deadline_missed") return hint;
  return legacyState === "pending" ? "ready" : "unknown";
}

export function canReplayTask(task: TaskRow): boolean {
  return REPLAYABLE.has(String(task.outcome || task.status || "")) &&
    Boolean(task.action && task.payload_descriptor && typeof task.payload_descriptor === "object");
}

export function taskError(task: TaskRow): string {
  const error = task.last_error ?? task.error ?? task.reason;
  if (typeof error === "string") return error;
  if (error && typeof error === "object") {
    const detail = error as Record<string, unknown>;
    return String(detail.reason ?? detail.message ?? JSON.stringify(error));
  }
  return "";
}

export function replayIds(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String).filter(Boolean);
  if (typeof value !== "string" || !value.trim()) return [];
  if (value.trim().startsWith("[")) {
    try {
      const parsed: unknown = JSON.parse(value);
      if (Array.isArray(parsed)) return parsed.map(String).filter(Boolean);
    } catch { /* 旧文本继续按逗号分隔读取。 */ }
  }
  return [...new Set(value.split(",").map((id) => id.trim()).filter(Boolean))];
}
