import { useEffect, useRef, type RefObject } from "react";

interface Layer { root: HTMLElement }
const layers: Layer[] = [];
let previousOverflow = "";
const focusable = (root: HTMLElement) => [...root.querySelectorAll<HTMLElement>(
  "a[href],button,input,select,textarea,[tabindex]",
)].filter(element => element.tabIndex >= 0 && !element.matches(":disabled") &&
  !element.closest("[inert],[aria-hidden='true']") && element.getClientRects().length > 0 &&
  getComputedStyle(element).visibility !== "hidden");
const focus = (element: HTMLElement) => element.focus({ preventScroll: true });

/** 只有最上层模态处理键盘；轮询和回调变化不会重新进入或重置焦点。 */
export function useModalFocus(ref: RefObject<HTMLElement>, {
  open = true, busy = false, onClose,
}: { open?: boolean; busy?: boolean; onClose: () => void }) {
  const current = useRef({ busy, onClose });
  current.current = { busy, onClose };
  useEffect(() => {
    const root = ref.current;
    if (!open || !root) return;
    const previous = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const layer = { root };
    if (layers.length === 0) {
      previousOverflow = document.body.style.overflow;
      document.body.style.overflow = "hidden";
    }
    layers.push(layer);
    const isTop = () => layers[layers.length - 1] === layer;
    const enter = () => focus(focusable(root)[0] ?? root);
    const onFocus = (event: FocusEvent) => {
      if (isTop() && !root.contains(event.target as Node)) enter();
    };
    const onKey = (event: KeyboardEvent) => {
      if (!isTop()) return;
      if (event.key === "Escape") {
        event.preventDefault();
        event.stopImmediatePropagation();
        if (!current.current.busy) current.current.onClose();
      } else if (event.key === "Tab" && !event.ctrlKey && !event.altKey && !event.metaKey) {
        const targets = focusable(root);
        const first = targets[0];
        const last = targets[targets.length - 1];
        if (!first) { event.preventDefault(); focus(root); }
        else if (!root.contains(document.activeElement) || document.activeElement === root) {
          event.preventDefault(); focus(event.shiftKey ? last : first);
        } else if (event.shiftKey && document.activeElement === first) {
          event.preventDefault(); focus(last);
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault(); focus(first);
        }
      }
    };
    document.addEventListener("focusin", onFocus, true);
    document.addEventListener("keydown", onKey, true);
    enter();
    return () => {
      const wasTop = isTop();
      document.removeEventListener("focusin", onFocus, true);
      document.removeEventListener("keydown", onKey, true);
      const index = layers.indexOf(layer);
      if (index >= 0) layers.splice(index, 1);
      if (layers.length === 0) document.body.style.overflow = previousOverflow;
      if (!wasTop) return;
      const parent = layers[layers.length - 1]?.root;
      if (previous?.isConnected && (!parent || parent.contains(previous))) focus(previous);
      else if (parent) focus(focusable(parent)[0] ?? parent);
    };
  }, [open, ref]);
  useEffect(() => {
    const root = ref.current;
    if (open && root && layers[layers.length - 1]?.root === root &&
      (!root.contains(document.activeElement) || document.activeElement === root)) {
      focus(focusable(root)[0] ?? root);
    }
  }, [busy, open, ref]);
}
