import { useSyncExternalStore } from "react";

/** Dark by default; the choice is remembered per browser. index.html applies it before
 *  the first paint, so this module only has to keep React and the document in sync. */
export type Theme = "dark" | "light";

const KEY = "feedwatch-theme";
const listeners = new Set<() => void>();

function current(): Theme {
  return document.documentElement.dataset.theme === "light" ? "light" : "dark";
}

export function setTheme(theme: Theme): void {
  document.documentElement.dataset.theme = theme;
  try {
    localStorage.setItem(KEY, theme);
  } catch {
    /* private mode: the toggle still works for this visit */
  }
  for (const l of listeners) l();
}

export function useTheme(): Theme {
  return useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    current,
  );
}

/** Chart colours come from the same CSS tokens as the rest of the page. */
export function chartPalette() {
  const css = getComputedStyle(document.documentElement);
  const v = (name: string) => css.getPropertyValue(name).trim();
  return {
    up: v("--ok"),
    down: v("--critical"),
    warning: v("--warning"),
    critical: v("--critical"),
    info: v("--info"),
    grid: v("--chart-grid"),
    text: v("--chart-text"),
  };
}
