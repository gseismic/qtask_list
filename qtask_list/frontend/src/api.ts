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
  let data;
  try { data = await response.json(); }
  catch {
    if (response.ok) throw new Error("服务返回了无法识别的响应，请刷新核对操作结果");
    data = {};
  }
  if (!response.ok) {
    const detail = data.detail ?? response.statusText;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return data;
}

const q = (name: string) => encodeURIComponent(name);
const queuePath = (name: string, operation: string) => `/api/queues/${operation}?name=${q(name)}`;
const taskPath = (id: string, operation: string) => `/api/tasks/${operation}?task_id=${q(id)}`;

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
  diagnose: (queue: string) => request<DiagnoseInfo>(queuePath(queue, "diagnose")),
  expired: (queue: string, limit = 50) => request(`${queuePath(queue, "expired")}&limit=${limit}`),
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
    return request<TaskPageResult>(`${queuePath(opts.queue, "tasks")}&${params.toString()}`);
  },
  pushTask: (queue: string, body: PushTaskBody) =>
    request(queuePath(queue, "tasks"), { method: "POST", body: JSON.stringify(body) }),
  retryQueue: (queue: string) => request(queuePath(queue, "retry"), { method: "POST" }),
  requeueDlq: (queue: string, taskId: string | null = null) =>
    request(queuePath(queue, "requeue-dlq"), {
      method: "POST",
      body: JSON.stringify({ task_id: taskId, confirm_bulk: taskId === null }),
    }),
  requeueExpired: (queue: string, startDeadlineAt: string, taskId: string | null = null) =>
    request(queuePath(queue, "requeue-expired"), {
      method: "POST",
      body: JSON.stringify({ task_id: taskId, start_deadline_at: startDeadlineAt }),
    }),
  recoverQueue: (queue: string, includeActive = false, workerId?: string) =>
    request(queuePath(queue, "recover"), {
      method: "POST",
      body: JSON.stringify({ include_active: includeActive, confirm_active: includeActive, worker_id: workerId }),
    }),
  clearQueue: (queue: string, includeDlq: boolean, includeHistory: boolean, releaseIdentity: boolean) =>
    request(queuePath(queue, "clear"), {
      method: "POST",
      body: JSON.stringify({
        include_dlq: includeDlq,
        include_history: includeHistory,
        identity_policy: releaseIdentity ? "release" : "keep",
        confirm_identity_release: releaseIdentity,
      }),
    }),
  cleanHistory: (queue: string, ttlDays: number) =>
    request(`${queuePath(queue, "clean-history")}&ttl_days=${ttlDays}`, { method: "POST" }),
  deleteQueue: (queue: string) =>
    request(`${queuePath(queue, "detail")}&confirm=true`, { method: "DELETE" }),
  task: (taskId: string) => request<TaskRow>(taskPath(taskId, "detail")),
  taskPayload: (taskId: string, queue: string, state = "all") =>
    request(`${taskPath(taskId, "payload")}&queue=${q(queue)}&state=${q(state)}`),
  requeueTask: (taskId: string, queue: string, fromState: string) =>
    request(taskPath(taskId, "requeue"), {
      method: "POST",
      body: JSON.stringify({ queue, from_state: fromState }),
    }),
  replayTask: (taskId: string, body: Record<string, unknown>) =>
    request(taskPath(taskId, "replay"), {
      method: "POST",
      body: JSON.stringify(body),
    }),
  deleteTask: (taskId: string, queue = "") =>
    request(`${taskPath(taskId, "detail")}${queue ? `&queue=${q(queue)}` : ""}`, {
      method: "DELETE",
    }),
};
