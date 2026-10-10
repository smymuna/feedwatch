/** Small, pure formatting helpers (tested in format.test.ts). */

export function ago(iso: string | null, now: number): string {
  if (!iso) return "never";
  const s = Math.max(0, Math.round((now - Date.parse(iso)) / 1000));
  if (s < 60) return `${s} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  return `${Math.floor(s / 3600)} h ago`;
}

export function duration(fromIso: string, toIso: string | null, now: number): string {
  const ms = (toIso ? Date.parse(toIso) : now) - Date.parse(fromIso);
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return `${s} s`;
  if (s < 3600) return `${Math.floor(s / 60)} min ${s % 60} s`;
  return `${Math.floor(s / 3600)} h ${Math.floor((s % 3600) / 60)} min`;
}

/** Prices: enough digits to see a stablecoin move, but not noise. */
export function price(value: string | null): string {
  if (value === null) return "–";
  const n = Number(value);
  if (n >= 1000) return n.toLocaleString("en-US", { maximumFractionDigits: 2 });
  if (n >= 10) return n.toFixed(3);
  return n.toFixed(5);
}

export function pct(value: number | null, digits = 2): string {
  if (value === null) return "–";
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}${Math.abs(value).toFixed(digits)} %`;
}

export function rate(perSecond: number): string {
  return perSecond >= 10 ? perSecond.toFixed(0) : perSecond.toFixed(1);
}

/** Position of a peg deviation on a ±range gauge, as 0–100 %. */
export function gaugePosition(devPct: number, range = 2): number {
  return 50 + (Math.max(-range, Math.min(range, devPct)) / range) * 50;
}

export function pegLevel(devPct: number | null, warn = 0.5, critical = 2): "ok" | "warning" | "critical" | "none" {
  if (devPct === null) return "none";
  const d = Math.abs(devPct);
  return d >= critical ? "critical" : d >= warn ? "warning" : "ok";
}

export const TYPE_LABEL: Record<string, string> = {
  feed_silent: "Feed silent",
  trade_gap: "Missing trades",
  price_divergence: "Price divergence",
  depeg: "Depeg",
  outlier_print: "Bad print",
  high_latency: "High latency",
};
