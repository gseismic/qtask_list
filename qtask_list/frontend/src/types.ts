export interface QueueStats {
  queue: number;
  processing: number;
  retry: number;
  retry_wait: number;
  dlq: number;
  delay: number;
  history: number;
  completed: number;
  completed_total: number;
  failed: number;
  failed_total: number;
  skipped: number;
  cancelled: number;
  deadline_missed: number;
  expired: number;
  active_workers: number;
  stale_workers: number;
  observation_indexed?: number;
}

export interface QueueInfo {
  name: string;
  [key: string]: number | string;
}

export interface WorkerInfo {
  queue: string;
  worker_id: string;
  active: boolean;
  heartbeat_key: string;
  ttl: number;
  last_seen: number | null;
  processing_key: string;
  processing: number;
}

export interface HealthInfo {
  status: string;
  redis?: string;
  error?: string;
  memory?: {
    used_memory_human?: string;
    used_memory_peak_human?: string;
    maxmemory_human?: string;
    used_memory?: number;
    maxmemory?: number;
    status?: string;
    threshold_bytes?: number | null;
    threshold_source?: string | null;
  };
}

export interface TaskRow {
  task_id?: string;
  action?: string;
  attempt?: number;
  max_attempts?: number;
  state?: string;
  status?: string;
  outcome?: string;
  location?: string;
  delay_reason?: string;
  last_error?: unknown;
  last_error_code?: string;
  reason?: string;
  reason_code?: string;
  available_at?: number | string;
  start_deadline_at?: number | string;
  expires_at?: number | string;
  payload_descriptor?: Record<string, unknown>;
  created_at?: number | string;
  updated_at?: number | string;
  started_at?: number | string;
  last_started_at?: number | string;
  finished_at?: number | string;
  run_at?: number;
  run_at_text?: string;
  logical_key?: string;
  replay_of?: string;
  replayed_by?: string | string[];
  worker?: string;
  error?: string;
  payload?: unknown;
  result?: unknown;
  _queue?: string;
  _state?: string;
  _note?: string;
  [key: string]: unknown;
}

export interface DiagnoseInfo {
  queue: string;
  stats: QueueStats;
  workers: WorkerInfo[];
  suggestions: string[];
}

export interface TaskPageResult {
  tasks: TaskRow[];
  /** 仅第一页返回服务端确认的精确 ID，与列表筛选独立。 */
  exact_task_id?: string | null;
  next_cursor?: string | null;
  scan_limited?: boolean;
}

export const STATE_KEYS = [
  "all",
  "ready",
  "processing",
  "retry",
  "retry_wait",
  "delay",
  "dlq",
  "completed",
  "failed",
  "skipped",
  "cancelled",
  "deadline_missed",
  "history",
] as const;
export type StateKey = (typeof STATE_KEYS)[number];

export const STATE_LABELS: Record<StateKey, string> = {
  all: "全部",
  ready: "排队中",
  processing: "执行中",
  retry: "手动重试",
  retry_wait: "等待重试",
  delay: "延迟",
  dlq: "死信",
  completed: "完成",
  failed: "失败",
  skipped: "跳过",
  cancelled: "取消",
  deadline_missed: "过期",
  history: "历史",
};

export function statForState(stats: QueueStats, state: StateKey): number {
  const map: Record<StateKey, keyof QueueStats> = {
    all: -1 as unknown as keyof QueueStats,
    ready: "queue",
    processing: "processing",
    retry: "retry",
    retry_wait: "retry_wait",
    delay: "delay",
    dlq: "dlq",
    completed: "completed",
    failed: "failed",
    skipped: "skipped",
    cancelled: "cancelled",
    deadline_missed: "deadline_missed",
    history: "history",
  };
  if (state === "all") return -1;
  return stats[map[state]] ?? 0;
}

export type Severity = "danger" | "warning";
