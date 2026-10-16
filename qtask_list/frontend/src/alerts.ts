import type { HealthInfo, QueueInfo, Severity, WorkerInfo } from "./types";

export interface AlertItem {
  key: string;
  rule: string;
  ruleLabel: string;
  severity: Severity;
  target: string;
  detail: string;
  firstSeen: number;
  locate: { page: "queue" | "workers" | "tasks"; queue?: string; state?: string };
}

export const ALERT_RULES_DOC = [
  { name: "DLQ 堆积", rule: "dlq>0", severity: "danger", source: "queue_stats.dlq", note: "有任务重试耗尽进入死信，需人工处理" },
  { name: "无人消费", rule: "ready>0 且 active_workers=0", severity: "danger", source: "queue_stats.queue/active_workers", note: "ready 有任务，但没有在线 Worker；纯延迟队列不触发" },
  { name: "失败率偏高", rule: "failed/(failed+completed) > 5%", severity: "warning", source: "queue_stats 累计计数", note: "历史累计口径；旧故障恢复后仍可能持续显示" },
  { name: "Stale Worker", rule: "stale_workers>0", severity: "danger", source: "queue_stats.stale_workers", note: "Worker 失联但 processing 还有任务，可恢复" },
  { name: "ready 积压", rule: "ready>10000 持续 10min", severity: "warning", source: "queue_stats.queue", note: "消费能力不足或 Worker 未启动" },
  { name: "任务过期", rule: "deadline_missed>0", severity: "warning", source: "queue_stats.deadline_missed", note: "任务超过执行截止时间，可改截止时间重放" },
  { name: "Worker 心跳超时", rule: "processing>0 且心跳不存在", severity: "danger", source: "list_workers", note: "任务变孤儿，可恢复回主队列" },
  { name: "Redis 内存超限", rule: "超过 monitor_threshold_mb", severity: "danger", source: "/api/health", note: "检查 maxmemory 与清理策略" },
];

const READY_THRESHOLD = 10000;
const READY_SUSTAIN_MS = 10 * 60 * 1000;
const FAILED_RATIO = 0.05;

const RESOLVED_KEY = "qtask.alerts.resolved.v2";
const LEGACY_RESOLVED_KEY = "qtask.alerts.resolved.v1";
interface Resolution { at: number; active: boolean }

function loadResolutions(): Record<string, Resolution> {
  try {
    const stored = localStorage.getItem(RESOLVED_KEY);
    const parsed: unknown = JSON.parse(stored ?? localStorage.getItem(LEGACY_RESOLVED_KEY) ?? "{}");
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    const entries: Array<[string, Resolution]> = [];
    for (const [key, value] of Object.entries(parsed)) {
      if (!stored && typeof value === "number" && Number.isFinite(value)) entries.push([key, { at: value, active: true }]);
      else if (value && typeof value === "object" && typeof value.at === "number" && Number.isFinite(value.at)) {
        entries.push([key, { at: value.at, active: value.active === true }]);
      }
    }
    return Object.fromEntries(entries);
  } catch {
    return {};
  }
}

function saveResolved(map: Record<string, Resolution>) {
  localStorage.setItem(RESOLVED_KEY, JSON.stringify(map));
}

export function resolveAlert(key: string) {
  const map = loadResolutions();
  map[key] = { at: Date.now(), active: true };
  saveResolved(map);
}

export function clearResolved() {
  saveResolved({});
}

export function loadResolved(): Record<string, number> {
  return Object.fromEntries(Object.entries(loadResolutions()).map(([key, value]) => [key, value.at]));
}

interface FiringState {
  since: number;
  delayMs: number;
  alert: Omit<AlertItem, "firstSeen">;
}

export class AlertEngine {
  private firing = new Map<string, FiringState>();

  compute(queues: QueueInfo[], workers: WorkerInfo[], health: HealthInfo | null): AlertItem[] {
    const now = Date.now();

    for (const item of queues) {
      const stats = item as unknown as Record<string, number>;
      const name = String(item.name);
      if ((stats.dlq ?? 0) > 0) {
        this.fire(`dlq:${name}`, {
          key: `dlq:${name}`,
          rule: "dlq",
          ruleLabel: "DLQ 堆积",
          severity: "danger",
          target: name,
          detail: `死信队列有 ${stats.dlq} 条任务等待处理`,
          locate: { page: "queue", queue: name, state: "dlq" },
        });
      }
      if ((stats.queue ?? 0) > 0 && (stats.active_workers ?? 0) === 0) {
        this.fire(`noworker:${name}`, {
          key: `noworker:${name}`,
          rule: "noworker",
          ruleLabel: "无人消费",
          severity: "danger",
          target: name,
          detail: `ready 有 ${stats.queue} 条任务，但没有在线 Worker`,
          locate: { page: "queue", queue: name, state: "ready" },
        });
      }
      const done = (stats.completed_total ?? 0) + (stats.failed_total ?? 0);
      if (done >= 20 && (stats.failed_total ?? 0) / done > FAILED_RATIO) {
        const pct = Math.round(((stats.failed_total ?? 0) / done) * 100);
        this.fire(`failedratio:${name}`, {
          key: `failedratio:${name}`,
          rule: "failedratio",
          ruleLabel: "失败率偏高",
          severity: "warning",
          target: name,
          detail: `累计失败率 ${pct}%（>5%）`,
          locate: { page: "queue", queue: name, state: "failed" },
        });
      }
      if ((stats.stale_workers ?? 0) > 0) {
        this.fire(`stale:${name}`, {
          key: `stale:${name}`,
          rule: "stale",
          ruleLabel: "Stale Worker",
          severity: "danger",
          target: name,
          detail: `${stats.stale_workers} 个失联 Worker 仍有任务在 processing`,
          locate: { page: "queue", queue: name, state: "processing" },
        });
      }
      if ((stats.queue ?? 0) > READY_THRESHOLD) {
        this.fire(
          `ready:${name}`,
          {
            key: `ready:${name}`,
            rule: "ready",
            ruleLabel: "ready 积压",
            severity: "warning",
            target: name,
            detail: `ready ${stats.queue.toLocaleString("zh-CN")} 条，超过 10000 持续 10 分钟`,
            locate: { page: "queue", queue: name, state: "ready" },
          },
          READY_SUSTAIN_MS,
        );
      }
      if ((stats.deadline_missed ?? 0) > 0) {
        this.fire(`expired:${name}`, {
          key: `expired:${name}`,
          rule: "expired",
          ruleLabel: "任务过期",
          severity: "warning",
          target: name,
          detail: `${stats.deadline_missed} 个任务超过执行截止时间`,
          locate: { page: "queue", queue: name, state: "deadline_missed" },
        });
      }
    }

    for (const worker of workers) {
      if (!worker.active && worker.processing > 0) {
        const key = `workerlost:${worker.queue}:${worker.worker_id}`;
        this.fire(key, {
          key,
          rule: "workerlost",
          ruleLabel: "Worker 失联",
          severity: "danger",
          target: `${worker.queue} / ${worker.worker_id}`,
          detail: `心跳超时，processing 中有 ${worker.processing} 个任务可恢复`,
          locate: { page: "workers" },
        });
      }
    }

    if (health?.memory?.status === "warning") {
      this.fire("redismem", {
        key: "redismem",
        rule: "redismem",
        ruleLabel: "Redis 内存超限",
        severity: "danger",
        target: health.redis ?? "Redis",
        detail: `已用 ${health.memory.used_memory_human ?? "?"}，超过阈值`,
        locate: { page: "workers" },
      });
    }

    this.expireStale(queues, workers, health, now);
    const resolved = loadResolutions();
    let changed = false;
    for (const [key, resolution] of Object.entries(resolved)) {
      if (resolution.active && !this.firing.has(key)) {
        resolution.active = false;
        changed = true;
      }
    }
    // 告警恢复不因本地存储不可写而使整页监控失败；手工确认仍显式反馈写入失败。
    if (changed) { try { saveResolved(resolved); } catch { /* 下次观测重试保存。 */ } }
    const alerts: AlertItem[] = [];
    for (const [key, state] of this.firing.entries()) {
      if (now - state.since < state.delayMs) continue;
      if (resolved[key]?.active) continue;
      alerts.push({ ...state.alert, firstSeen: state.since });
    }

    alerts.sort((a, b) => {
      const rank = (s: Severity) => (s === "danger" ? 0 : 1);
      return rank(a.severity) - rank(b.severity) || a.firstSeen - b.firstSeen;
    });
    return alerts;
  }

  private fire(key: string, alert: Omit<AlertItem, "firstSeen">, delayMs = 0) {
    const existing = this.firing.get(key);
    if (!existing) this.firing.set(key, { since: Date.now(), delayMs, alert });
    else this.firing.set(key, { ...existing, alert });
  }

  private expireStale(queues: QueueInfo[], workers: WorkerInfo[], health: HealthInfo | null, now: number) {
    void now;
    const validKeys = new Set<string>();
    for (const item of queues) {
      const stats = item as unknown as Record<string, number>;
      const name = String(item.name);
      if ((stats.dlq ?? 0) > 0) validKeys.add(`dlq:${name}`);
      if ((stats.queue ?? 0) > 0 && (stats.active_workers ?? 0) === 0) validKeys.add(`noworker:${name}`);
      const done = (stats.completed_total ?? 0) + (stats.failed_total ?? 0);
      if (done >= 20 && (stats.failed_total ?? 0) / done > FAILED_RATIO) validKeys.add(`failedratio:${name}`);
      if ((stats.stale_workers ?? 0) > 0) validKeys.add(`stale:${name}`);
      if ((stats.queue ?? 0) > READY_THRESHOLD) validKeys.add(`ready:${name}`);
      if ((stats.deadline_missed ?? 0) > 0) validKeys.add(`expired:${name}`);
    }
    for (const worker of workers) {
      if (!worker.active && worker.processing > 0) {
        validKeys.add(`workerlost:${worker.queue}:${worker.worker_id}`);
      }
    }
    if (health?.memory?.status === "warning") validKeys.add("redismem");
    for (const key of [...this.firing.keys()]) {
      if (!validKeys.has(key)) this.firing.delete(key);
    }
  }
}

export const alertEngine = new AlertEngine();
