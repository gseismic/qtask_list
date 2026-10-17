/** 批量操作的部分完成必须留在当前确认上下文中。 */
export function summarizeBulkResult(value: unknown): { message: string; warning: boolean } | null {
  if (!value || typeof value !== "object") return null;
  const result = value as Record<string, unknown>;
  if (typeof result.moved !== "number" || typeof result.skipped !== "number") return null;
  const unattempted = typeof result.not_attempted === "number" ? result.not_attempted : 0;
  const parts = [`已处理 ${result.moved} 条，跳过 ${result.skipped} 条${unattempted ? `，未尝试 ${unattempted} 条` : ""}。`];
  if (Array.isArray(result.errors)) for (const item of result.errors) {
    if (item && typeof item === "object" && typeof item.reason === "string") {
      parts.push(`${item.task_id ? `${String(item.task_id)}：` : ""}${item.reason}`);
    }
  }
  if (result.errors_truncated) parts.push("仅显示前 20 项原因；其余未处理任务仍保留在死信中。");
  if (result.skipped || unattempted) parts.push("未处理消息已保留。截止已过的任务请单独更新截止时间后重放。");
  return { message: parts.join("\n"), warning: result.skipped > 0 || unattempted > 0 };
}
