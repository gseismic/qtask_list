import { useEffect, useState } from "react";

/** 只保存界面偏好；存储不可用时仍允许当前页面正常操作。 */
export function useStoredPreference<T>(key: string, initial: T, valid: (value: unknown) => value is T) {
  const [value, setValue] = useState<T>(() => {
    try {
      const saved: unknown = JSON.parse(localStorage.getItem(key) ?? "null");
      return valid(saved) ? saved : initial;
    } catch { return initial; }
  });
  useEffect(() => {
    try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* 偏好存储失败不影响业务操作。 */ }
  }, [key, value]);
  return [value, setValue] as const;
}
