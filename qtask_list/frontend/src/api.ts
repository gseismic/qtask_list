import type { DiagnoseInfo, HealthInfo, QueueInfo, TaskPageResult, TaskRow, WorkerInfo } from "./types";

async function request<T = any>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
  });
  if (response.status === 401 && !path.startsWith("/api/login") && !path.startsWith("/api/auth")) {
    const next = `${window.location.pathname}${window.location.search}`;
    window.location.href = `/login?next=${encodeURIComponent(next)}`;
    throw new Error("未登录");
  }
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = data.detail ?? response.statusText;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return data;
}

const q = (name: string) => encodeURIComponent(name);

export interface PushTaskBody {
  payload: Record<string, unknown>;
  action?: string | null;
  delay_seconds?: number;
  expire_seconds?: number;
  logical_key?: string | null;
  dedup_until?: string | null;
  duplicate_action?: string;
  confirm_duplicate?: boolean;
}

export const api = {
  auth: () => request("/api/auth"),
  logout: () => request("/api/logout", { method: "POST" }),
  health: () => request<HealthInfo>("/api/health"),
  queues: () => request<QueueInfo[]>("/api/queues"),
  workers: (queue = "") => request<WorkerInfo[]>(`/api/workers${queue ? `?queue=${q(queue)}` : ""}`),
  diagnose: (queue: string) => request<DiagnoseInfo>(`/api/queue/${q(queue)}/diagnose`),
  expired: (queue: string, limit = 50) => request(`/api/queue/${q(queue)}/expired?limit=${limit}`),
  tasks: (opts: {
    state: string;
    action?: string;
    search?: string;
    limit?: number;
    cursor?: string;
    createdAfter?: number;
    createdBefore?: number;
    completedAfter?: number;
    completedBefore?: number;
  }) => {
    const params = new URLSearchParams({ status: opts.state, limit: String(opts.limit ?? 50) });
    if (opts.action) params.set("action", opts.action);
    if (opts.search) params.set("search", opts.search);
    if (opts.cursor) params.set("cursor", opts.cursor);
    if (opts.createdAfter !== undefined) params.set("created_after", String(opts.createdAfter));
    if (opts.createdBefore !== undefined) params.set("created_before", String(opts.createdBefore));
    if (opts.completedAfter !== undefined) params.set("completed_after", String(opts.completedAfter));
    if (opts.completedBefore !== undefined) params.set("completed_before", String(opts.completedBefore));
    return request<TaskPageResult>(`/api/tasks?${params.toString()}`);
  },
  queueTasks: (opts: {
    queue: string;
    state: string;
    search?: string;
    action?: string;
    limit?: number;
    cursor?: string;
    createdAfter?: number;
    createdBefore?: number;
    completedAfter?: number;
    completedBefore?: number;
  }) => {
    const params = new URLSearchParams({ state: opts.state, limit: String(opts.limit ?? 50) });
    if (opts.action) params.set("action", opts.action);
    if (opts.search) params.set("search", opts.search);
    if (opts.cursor) params.set("cursor", opts.cursor);
    if (opts.createdAfter !== undefined) params.set("created_after", String(opts.createdAfter));
    if (opts.createdBefore !== undefined) params.set("created_before", String(opts.createdBefore));
    if (opts.completedAfter !== undefined) params.set("completed_after", String(opts.completedAfter));
    if (opts.completedBefore !== undefined) params.set("completed_before", String(opts.completedBefore));
    return request<TaskPageResult>(`/api/queue/${q(opts.queue)}/tasks?${params.toString()}`);
  },
  pushTask: (queue: string, body: PushTaskBody) =>
    request(`/api/queue/${q(queue)}/tasks`, { method: "POST", body: JSON.stringify(body) }),
  retryQueue: (queue: string) => request(`/api/queue/${q(queue)}/retry`, { method: "POST" }),
  requeueDlq: (queue: string, taskId: string | null = null) =>
    request(`/api/queue/${q(queue)}/requeue-dlq`, {
      method: "POST",
      body: JSON.stringify({ task_id: taskId, confirm_bulk: taskId === null }),
    }),
  requeueExpired: (queue: string, startDeadlineAt: string, taskId: string | null = null) =>
    request(`/api/queue/${q(queue)}/requeue-expired`, {
      method: "POST",
      body: JSON.stringify({ task_id: taskId, start_deadline_at: startDeadlineAt }),
    }),
  recoverQueue: (queue: string, includeActive = false, workerId?: string) =>
    request(`/api/queue/${q(queue)}/recover`, {
      method: "POST",
      body: JSON.stringify({ include_active: includeActive, confirm_active: includeActive, worker_id: workerId }),
    }),
  clearQueue: (queue: string, includeDlq: boolean, includeHistory: boolean, releaseIdentity: boolean) =>
    request(`/api/queue/${q(queue)}/clear`, {
      method: "POST",
      body: JSON.stringify({
        include_dlq: includeDlq,
        include_history: includeHistory,
        identity_policy: releaseIdentity ? "release" : "keep",
        confirm_identity_release: releaseIdentity,
      }),
    }),
  cleanHistory: (queue: string, ttlDays: number) =>
    request(`/api/queue/${q(queue)}/clean-history?ttl_days=${ttlDays}`, { method: "POST" }),
  deleteQueue: (queue: string) =>
    request(`/api/queue/${q(queue)}?confirm=true`, { method: "DELETE" }),
  task: (taskId: string) => request<TaskRow>(`/api/task/${encodeURIComponent(taskId)}`),
  taskPayload: (taskId: string, queue: string, state = "all") =>
    request(`/api/task/${encodeURIComponent(taskId)}/payload?queue=${q(queue)}&state=${state}`),
  requeueTask: (taskId: string, queue: string, fromState: string) =>
    request(`/api/task/${encodeURIComponent(taskId)}/requeue`, {
      method: "POST",
      body: JSON.stringify({ queue, from_state: fromState }),
    }),
  replayTask: (taskId: string, body: Record<string, unknown>) =>
    request(`/api/task/${encodeURIComponent(taskId)}/replay`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  deleteTask: (taskId: string, queue = "") =>
    request(`/api/task/${encodeURIComponent(taskId)}${queue ? `?queue=${q(queue)}` : ""}`, {
      method: "DELETE",
    }),
};
