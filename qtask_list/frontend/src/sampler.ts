import type { QueueInfo } from "./types";

export interface Sample {
  ts: number;
  completed: number;
  observationIndexed: number;
  runnable: boolean; // 本次观测有可消费积压，防止把空闲时间算进停滞。
}

export interface QueueProgress {
  remaining: number;
  completedObserved: number | null;
  ratePerMin: number | null;
  etaText: string | null;
  stalled: boolean;
}

const WINDOW_MS = 60 * 60 * 1000;
const MIN_RATE_MS = 30 * 1000;
const STALL_MS = 2 * 60 * 1000;
const MAX_OBSERVATION_GAP_MS = 60 * 1000;

function remainingOf(stats: Record<string, number>): number {
  return (stats.queue ?? 0) + (stats.processing ?? 0) + (stats.retry ?? 0) + (stats.delay ?? 0);
}

export class RateSampler {
  private samples = new Map<string, Sample[]>();

  observe(queues: QueueInfo[]) {
    const now = Date.now();
    const seen = new Set<string>();
    for (const item of queues) {
      const name = String(item.name);
      seen.add(name);
      const arr = this.samples.get(name) ?? [];
      const completed = Number(item.completed_total ?? 0);
      const observationIndexed = Number(item.observation_indexed ?? 0);
      const runnable = Number(item.queue ?? 0) > 0 && Number(item.active_workers ?? 0) > 0;
      if (arr.length && (completed < arr[arr.length - 1].completed ||
        observationIndexed !== arr[arr.length - 1].observationIndexed)) arr.length = 0;
      if (!arr.length || now - arr[arr.length - 1].ts >= 1000 ||
        completed !== arr[arr.length - 1].completed || runnable !== arr[arr.length - 1].runnable) {
        arr.push({ ts: now, completed, observationIndexed, runnable });
      }
      while (arr.length > 0 && now - arr[0].ts > WINDOW_MS + 5 * 60 * 1000) arr.shift();
      this.samples.set(name, arr);
    }
    for (const key of this.samples.keys()) {
      if (!seen.has(key)) this.samples.delete(key);
    }
  }

  progress(name: string, stats: Record<string, number>): QueueProgress {
    const arr = this.samples.get(name) ?? [];
    const remaining = remainingOf(stats);
    const empty: QueueProgress = {
      remaining,
      completedObserved: null,
      ratePerMin: null,
      etaText: null,
      stalled: false,
    };
    if (arr.length < 2 || Number(stats.observation_indexed ?? 0) === 0) return empty;

    const last = arr[arr.length - 1];
    const hourAgo = last.ts - WINDOW_MS;
    let base: Sample | null = null;
    for (const s of arr) {
      if (s.ts <= hourAgo) base = s;
      else break;
    }
    const ref = base ?? arr[0];
    const completedObserved = Math.max(0, last.completed - ref.completed);
    const spanMs = last.ts - ref.ts;
    const ratePerMin = spanMs >= MIN_RATE_MS ? completedObserved / (spanMs / 60000) : null;
    let runnableSince: number | null = null;
    for (let i = 0; i < arr.length; i++) {
      const previous = arr[i - 1];
      if (!arr[i].runnable) runnableSince = null;
      else if (runnableSince === null || (previous &&
        (arr[i].completed > previous.completed || arr[i].ts - previous.ts > MAX_OBSERVATION_GAP_MS))) {
        runnableSince = arr[i].ts;
      }
    }
    const stalled = (stats.queue ?? 0) > 0 && (stats.active_workers ?? 0) > 0 &&
      runnableSince !== null && last.ts - runnableSince >= STALL_MS;

    let etaText: string | null = null;
    if (remaining > 0 && (stats.delay ?? 0) === 0 && ratePerMin !== null && ratePerMin > 0.01) {
      const minutes = remaining / ratePerMin;
      etaText = minutes < 90 ? `约${Math.round(minutes)}min` : `约${(minutes / 60).toFixed(1)}h`;
    }
    return { remaining, completedObserved, ratePerMin, etaText, stalled };
  }
}

export function fmtInt(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  return n.toLocaleString("zh-CN");
}

export function fmtRate(rate: number | null): string {
  if (rate === null) return "—";
  if (rate >= 100) return `${Math.round(rate)}/min`;
  return `${rate.toFixed(1)}/min`;
}
