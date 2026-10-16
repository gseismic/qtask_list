import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { api } from "./api";
import { usePolling, type RefreshInterval } from "./hooks";
import { alertEngine, clearResolved, resolveAlert, type AlertItem } from "./alerts";
import { RateSampler, type QueueProgress } from "./sampler";
import type { HealthInfo, QueueInfo, WorkerInfo } from "./types";

interface AppData {
  queues: QueueInfo[] | null;
  workers: WorkerInfo[] | null;
  health: HealthInfo | null;
  alerts: AlertItem[];
  error: string | null;
  loading: boolean;
  lastUpdated: number | null;
  interval: RefreshInterval;
  setInterval: (v: RefreshInterval) => void;
  reload: () => void;
  refreshVersion: number; // 顶栏与操作成功后的共享刷新信号。
  progress: Record<string, QueueProgress>; // 应用级采样，切换页面时保留。
  resolveAlert: (key: string) => void;
  clearResolved: () => void;
}

const Ctx = createContext<AppData | null>(null);

export function AppDataProvider({ children }: { children: ReactNode }) {
  const [interval, setIntervalState] = useState<RefreshInterval>(5);
  const [reloadKey, setReloadKey] = useState(0);
  const [sampler] = useState(() => new RateSampler());
  const [alertVersion, setAlertVersion] = useState(0);
  const reload = useCallback(() => setReloadKey((key) => key + 1), []);

  useEffect(() => {
    const changed = (event: StorageEvent) => {
      if (event.key === null || event.key.startsWith("qtask.alerts.resolved.")) setAlertVersion((v) => v + 1);
    };
    window.addEventListener("storage", changed);
    return () => window.removeEventListener("storage", changed);
  }, []);

  const { data, error, loading, lastUpdated } = usePolling(async () => {
    const [queues, workers, health] = await Promise.all([api.queues(), api.workers(), api.health()]);
    if (health.status !== "ok") throw new Error(health.error ?? "Redis 不可用");
    return { queues, workers, health };
  }, interval, [reloadKey], (snapshot) => sampler.observe(snapshot.queues));

  const progress = useMemo(() => Object.fromEntries((data?.queues ?? []).map((queue) => [
    String(queue.name), sampler.progress(String(queue.name), queue as unknown as Record<string, number>),
  ])), [data, sampler]);

  const alerts = useMemo(() => {
    if (!data) return [] as AlertItem[];
    return alertEngine.compute(data.queues, data.workers ?? [], data.health);
  }, [data, alertVersion]);

  const value: AppData = {
    queues: data?.queues ?? null,
    workers: data?.workers ?? null,
    health: data?.health ?? null,
    alerts,
    error: error ?? (data?.health.status === "error" ? data.health.error ?? "Redis 不可用" : null),
    loading,
    lastUpdated,
    interval,
    setInterval: (v) => setIntervalState(v),
    reload,
    refreshVersion: reloadKey,
    progress,
    resolveAlert: (key) => { resolveAlert(key); setAlertVersion((v) => v + 1); },
    clearResolved: () => { clearResolved(); setAlertVersion((v) => v + 1); },
  };

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useAppData(): AppData {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("AppData missing");
  return ctx;
}
