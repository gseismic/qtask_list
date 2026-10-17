import { useCallback, useEffect, useRef, useState } from "react";
import type { RefreshInterval } from "./hooks";
import type { TaskPageResult, TaskRow } from "./types";

/** 管理一个查询的第一页轮询与分页，防止跨筛选、跨刷新响应交错写入。 */
export function useTaskPage({ queryKey, fetchPage, interval, refreshVersion, onPause }: {
  queryKey: string;
  fetchPage: (cursor?: string) => Promise<TaskPageResult>;
  interval: RefreshInterval;
  refreshVersion: number;
  onPause: () => void;
}) {
  const [rows, setRows] = useState<TaskRow[] | null>(null);
  const [exactMatch, setExactMatch] = useState<{ queryKey: string; taskId: string | null } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [scanLimited, setScanLimited] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const current = useRef({ queryKey, fetchPage, onPause, nextCursor });
  current.current = { queryKey, fetchPage, onPause, nextCursor };
  const generation = useRef(0);
  const requestSeq = useRef(0);
  const inFlight = useRef(false);
  const errored = useRef(false);
  const previousQuery = useRef<string | null>(null); // 同查询重试保留旧数据，换查询时清空。

  const load = useCallback(async (cursor?: string) => {
    const key = current.current.queryKey;
    const queryGeneration = generation.current;
    const seq = ++requestSeq.current;
    const isCurrent = () => key === current.current.queryKey &&
      queryGeneration === generation.current && seq === requestSeq.current;
    inFlight.current = true;
    if (cursor) setLoadingMore(true);
    else setLoading(true);
    try {
      const page = await current.current.fetchPage(cursor);
      if (!isCurrent()) return;
      if (!cursor) setExactMatch({ queryKey: key, taskId: page.exact_task_id ?? null });
      setRows((previous) => {
        if (!cursor) return page.tasks;
        const merged = new Map<string, TaskRow>();
        for (const [index, row] of [...(previous ?? []), ...page.tasks].entries()) {
          merged.set(row.task_id ?? `unknown-${index}`, row);
        }
        return [...merged.values()];
      });
      setNextCursor(page.next_cursor ?? null);
      setScanLimited(Boolean(page.scan_limited));
      setLastUpdated(Date.now());
      setError(null);
      errored.current = false;
    } catch (cause) {
      if (isCurrent()) {
        setError(cause instanceof Error ? cause.message : String(cause));
        errored.current = true;
      }
    } finally {
      if (isCurrent()) {
        inFlight.current = false;
        setLoading(false);
        setLoadingMore(false);
      }
    }
  }, []);

  useEffect(() => {
    generation.current += 1;
    requestSeq.current += 1;
    inFlight.current = false;
    errored.current = false;
    const queryChanged = previousQuery.current !== queryKey;
    previousQuery.current = queryKey;
    if (queryChanged) {
      setRows(null);
      setExactMatch(null);
      setNextCursor(null);
      setScanLimited(false);
      setLastUpdated(null);
      setError(null);
    }
    setLoadingMore(false);
    void load();
    return () => { generation.current += 1; requestSeq.current += 1; };
  }, [queryKey, refreshVersion, refreshKey, load]);

  useEffect(() => {
    if (!interval) return;
    let stopped = false;
    let timer: number;
    const schedule = () => {
      timer = window.setTimeout(async () => {
        if (!document.hidden && !inFlight.current) await load();
        if (!stopped) schedule();
      }, (errored.current ? 30 : interval) * 1000);
    };
    schedule();
    return () => { stopped = true; window.clearTimeout(timer); };
  }, [interval, queryKey, refreshVersion, refreshKey, load]);

  const loadMore = () => {
    if (!current.current.nextCursor || inFlight.current) return;
    current.current.onPause();
    void load(current.current.nextCursor);
  };

  return { rows, exactTaskId: exactMatch?.queryKey === queryKey ? exactMatch.taskId : null,
    error, loading, loadingMore, nextCursor, scanLimited, lastUpdated,
    refresh: () => setRefreshKey((key) => key + 1), loadMore };
}
