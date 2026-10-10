/**
 * A TypeScript port of feedwatch/detect/detector.py for the browser edition.
 *
 * Same rules, same thresholds, same incident lifecycle (see docs/detection-rules.md), so
 * the public demo behaves like the real service. Differences: prices are JS numbers here
 * (the Python service uses exact decimals), and there's one browser instead of Kafka.
 * Parity is checked in detector.test.ts with the same scenarios as the Python tests.
 */

export type Kind = "trade" | "heartbeat" | "connection" | "tick";

export interface Trade {
  kind: "trade";
  source: string;
  symbol: string;
  tradeId: number | null;
  price: number;
  size: number;
  side: string;
  eventMs: number;
  ingestMs: number;
  snapshot?: boolean;
}
export interface Heartbeat {
  kind: "heartbeat";
  source: string;
  symbol: string | null;
  lastTradeId: number | null;
  eventMs: number | null;
  ingestMs: number;
}
export interface Connection {
  kind: "connection";
  source: string;
  state: "connected" | "subscribed" | "disconnected" | "error";
  ingestMs: number;
  detail?: string;
}
export interface Tick {
  kind: "tick";
  ingestMs: number;
}
export type Event = Trade | Heartbeat | Connection | Tick;

export type IncidentType = "feed_silent" | "trade_gap" | "price_divergence" | "depeg" | "outlier_print" | "high_latency";
export type Severity = "info" | "warning" | "critical";

export interface Incident {
  id: string;
  type: IncidentType;
  severity: Severity;
  source: string | null;
  symbol: string | null;
  openedMs: number;
  updatedMs: number;
  resolvedMs: number | null;
  summary: string;
  details: Record<string, unknown>;
}

export const THRESHOLDS = {
  silentAfterS: 10, divergenceBpsMajor: 50, divergenceBpsStable: 30, divergenceHoldS: 10, priceFreshS: 20,
  depegWarnPct: 0.5, depegCriticalPct: 2, depegResolvePct: 0.3, depegResolveHoldS: 60, depegWindowS: 60,
  depegMinTrades: 3, outlierWindow: 50, outlierMinBps: 50, outlierMadK: 10, latencyP95Ms: 3000, latencyWindowS: 60,
};
const BURST_QUIET_MS = 60_000;
const REEMIT_MS = 30_000;
const OUTLIER_MIN_HISTORY = 20;
const OUTLIER_CONFIRM = 2;
const DEESCALATE = 0.75;
export const STABLES = new Set(["USDT", "USDC", "DAI", "PYUSD", "USDS"]);
export const isStable = (symbol: string) => STABLES.has(symbol.split("-")[0]) && symbol.endsWith("-USD");
const fmtInt = (n: number) => n.toLocaleString("en-US");

export function median(values: number[]): number {
  const s = [...values].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
export function percentile(values: number[], q: number): number | null {
  if (!values.length) return null;
  const s = [...values].sort((a, b) => a - b);
  const rank = Math.max(0, Math.min(s.length - 1, Math.round((q / 100) * s.length + 0.5) - 1));
  return s[rank];
}

interface SourceState {
  connected: boolean;
  lastMessageMs: number | null;
  lastTradeMs: number | null;
  messages: number[];
  trades: number[];
  latency: [number, number][];
  gapsTotal: number;
  missingTotal: number;
  duplicatesTotal: number;
}
interface FeedState {
  window: [number, number, number][]; // [eventMs, price, size]
  recent: number[];
  lastTradeId: number | null;
  heartbeatsAhead: number;
  lastPrice: number | null;
  lastEventMs: number | null;
  pendingOutlier: { price: number; median: number; deviationBps: number; tradeId: number | null; confirmations: number } | null;
}

const key = (type: IncidentType, source: string | null, symbol: string | null) => `${type}|${source ?? "*"}|${symbol ?? "*"}`;

export class Detector {
  now = 0;
  startedMs: number | null = null;
  sources = new Map<string, SourceState>();
  feeds = new Map<string, FeedState>(); // "source|symbol"
  open = new Map<string, Incident>();
  private emitted = new Map<string, number>();
  private changes: Incident[] = [];
  private divSince = new Map<string, number>();
  divBps = new Map<string, number>();
  private pegOkSince = new Map<string, number>();
  private feedRules: boolean;

  constructor(sources: string[], opts: { feedRules?: boolean } = {}) {
    for (const s of sources) this.source(s);
    this.feedRules = opts.feedRules ?? true;
  }

  process(ev: Event): Incident[] {
    this.changes = [];
    this.now = Math.max(this.now, ev.ingestMs);
    if (this.startedMs === null) this.startedMs = this.now;
    if (ev.kind === "trade") this.onTrade(ev);
    else if (ev.kind === "heartbeat") {
      this.touch(ev.source);
      this.onHeartbeat(ev);
    } else if (ev.kind === "connection") this.onConnection(ev);
    this.timeRules();
    return this.changes;
  }

  // ------------------------------------------------------------------ state
  private source(name: string): SourceState {
    let st = this.sources.get(name);
    if (!st) {
      st = { connected: false, lastMessageMs: null, lastTradeMs: null, messages: [], trades: [], latency: [],
        gapsTotal: 0, missingTotal: 0, duplicatesTotal: 0 };
      this.sources.set(name, st);
    }
    return st;
  }
  feed(source: string, symbol: string): FeedState {
    const k = `${source}|${symbol}`;
    let fs = this.feeds.get(k);
    if (!fs) {
      fs = { window: [], recent: [], lastTradeId: null, heartbeatsAhead: 0, lastPrice: null, lastEventMs: null, pendingOutlier: null };
      this.feeds.set(k, fs);
    }
    return fs;
  }
  private touch(source: string) {
    const st = this.source(source);
    st.lastMessageMs = this.now;
    st.connected = true;
    st.messages.push(this.now);
    this.resolve(key("feed_silent", source, null), "Messages are flowing again.");
  }

  // ------------------------------------------------------------------ events
  private onConnection(ev: Connection) {
    const st = this.source(ev.source);
    if (ev.state === "disconnected") st.connected = false;
    else if (ev.state !== "error") this.touch(ev.source);
  }

  private onTrade(tr: Trade) {
    this.touch(tr.source);
    const st = this.source(tr.source);
    const fs = this.feed(tr.source, tr.symbol);
    this.checkSequence(tr, fs);
    if (tr.snapshot) return;
    st.trades.push(this.now);
    st.lastTradeMs = this.now;
    st.latency.push([this.now, tr.ingestMs - tr.eventMs]);
    this.checkOutlier(tr, fs);
    fs.recent.push(tr.price);
    if (fs.recent.length > THRESHOLDS.outlierWindow) fs.recent.shift();
    fs.window.push([tr.eventMs, tr.price, tr.size]);
    this.expire(fs, tr.eventMs);
    fs.lastPrice = tr.price;
    fs.lastEventMs = tr.eventMs;
    this.checkDivergence(tr.symbol, tr.eventMs);
    if (isStable(tr.symbol)) this.checkDepeg(tr.symbol, tr.eventMs);
  }

  private onHeartbeat(hb: Heartbeat) {
    if (hb.symbol === null || hb.lastTradeId === null) return;
    const fs = this.feed(hb.source, hb.symbol);
    if (fs.lastTradeId === null) {
      fs.lastTradeId = hb.lastTradeId;
      return;
    }
    if (hb.lastTradeId > fs.lastTradeId) {
      fs.heartbeatsAhead += 1;
      if (fs.heartbeatsAhead >= 2) {
        this.recordGap(hb.source, hb.symbol, fs.lastTradeId + 1, hb.lastTradeId);
        fs.lastTradeId = hb.lastTradeId;
        fs.heartbeatsAhead = 0;
      }
    } else fs.heartbeatsAhead = 0;
  }

  // ------------------------------------------------------------------ trade rules
  private checkSequence(tr: Trade, fs: FeedState) {
    if (tr.tradeId === null) return;
    const last = fs.lastTradeId;
    if (last === null || tr.tradeId === last + 1) fs.lastTradeId = tr.tradeId;
    else if (tr.tradeId > last + 1) {
      this.recordGap(tr.source, tr.symbol, last + 1, tr.tradeId - 1);
      fs.lastTradeId = tr.tradeId;
    } else this.source(tr.source).duplicatesTotal += 1;
    fs.heartbeatsAhead = 0;
  }

  private recordGap(source: string, symbol: string, first: number, last: number) {
    const missing = last - first + 1;
    const st = this.source(source);
    st.missingTotal += missing;
    const k = key("trade_gap", source, symbol);
    const inc = this.open.get(k);
    const lastGap = inc?.details.lastGap as [number, number] | undefined;
    const contiguous = !!lastGap && lastGap[1] + 1 === first;
    if (contiguous && lastGap) first = lastGap[0];
    else st.gapsTotal += 1;
    const gaps = ((inc?.details.gaps as number) ?? 0) + (contiguous ? 0 : 1);
    const total = ((inc?.details.missingTrades as number) ?? 0) + missing;
    this.upsert(k, "trade_gap", source, symbol, total >= 1000 ? "critical" : "warning",
      `${fmtInt(total)} ${symbol} trades missing from ${source} in ${gaps} gap(s)`,
      { gaps, missingTrades: total, lastGap: [first, last], lastHitMs: this.now }, true);
  }

  private dislocated(symbol: string) {
    return this.open.has(key("depeg", null, symbol)) || this.open.has(key("price_divergence", null, symbol));
  }

  private checkOutlier(tr: Trade, fs: FeedState) {
    const pending = fs.pendingOutlier;
    if (pending) {
      const back = (Math.abs(tr.price - pending.median) / pending.median) * 10_000 < THRESHOLDS.outlierMinBps / 2;
      if (!back) fs.pendingOutlier = null;
      else {
        pending.confirmations += 1;
        if (pending.confirmations >= OUTLIER_CONFIRM) {
          fs.pendingOutlier = null;
          const k = key("outlier_print", tr.source, tr.symbol);
          const count = ((this.open.get(k)?.details.count as number) ?? 0) + 1;
          this.upsert(k, "outlier_print", tr.source, tr.symbol, "info",
            `${count} ${tr.symbol} trade(s) on ${tr.source} far from the market, which ignored them`,
            { count, lastPrice: pending.price, median: pending.median, deviationBps: pending.deviationBps,
              tradeId: pending.tradeId, lastHitMs: this.now }, true);
        }
        return;
      }
    }
    if (fs.recent.length < OUTLIER_MIN_HISTORY || this.dislocated(tr.symbol)) return;
    const med = median(fs.recent);
    if (med <= 0) return;
    const mad = median(fs.recent.map((v) => Math.abs(v - med)));
    const dev = Math.abs(tr.price - med);
    const devBps = (dev / med) * 10_000;
    if (devBps > THRESHOLDS.outlierMinBps && dev > THRESHOLDS.outlierMadK * mad) {
      fs.pendingOutlier = { price: tr.price, median: med, deviationBps: Math.round(devBps * 10) / 10, tradeId: tr.tradeId, confirmations: 0 };
    }
  }

  private freshPrices(symbol: string, at: number): Map<string, number> {
    const out = new Map<string, number>();
    for (const [k, fs] of this.feeds) {
      const [source, sym] = k.split("|");
      if (sym !== symbol || fs.lastEventMs === null || !fs.recent.length) continue;
      if (at - fs.lastEventMs <= THRESHOLDS.priceFreshS * 1000) {
        const last5 = fs.recent.slice(-5).sort((a, b) => a - b);
        out.set(source, last5[last5.length >> 1]);
      }
    }
    return out;
  }

  private checkDivergence(symbol: string, at: number) {
    const prices = this.freshPrices(symbol, at);
    const k = key("price_divergence", null, symbol);
    if (prices.size < 2) {
      this.divBps.delete(symbol);
      return;
    }
    const values = [...prices.values()];
    const hi = Math.max(...values), lo = Math.min(...values);
    const bps = ((hi - lo) / ((hi + lo) / 2)) * 10_000;
    this.divBps.set(symbol, bps);
    const limit = isStable(symbol) ? THRESHOLDS.divergenceBpsStable : THRESHOLDS.divergenceBpsMajor;
    if (bps > limit) {
      if (!this.divSince.has(symbol)) this.divSince.set(symbol, at);
      const since = this.divSince.get(symbol) as number;
      if (at - since >= THRESHOLDS.divergenceHoldS * 1000) {
        let likely: string | null = null;
        if (isStable(symbol)) likely = [...prices].sort((a, b) => Math.abs(b[1] - 1) - Math.abs(a[1] - 1))[0][0];
        const current = this.open.get(k);
        const critical = bps > 3 * limit || (current?.severity === "critical" && bps > DEESCALATE * 3 * limit);
        const parts = [...prices].sort().map(([s, p]) => `${s} (${+p.toPrecision(6)})`).join(" and ");
        this.upsert(k, "price_divergence", null, symbol, critical ? "critical" : "warning",
          `${symbol} differs by ${bps.toFixed(0)} bps between ${parts}`,
          { bps: Math.round(bps * 10) / 10, prices: Object.fromEntries(prices), limitBps: limit, likelySource: likely, sinceMs: since });
      }
    } else if (bps < 0.8 * limit) {
      this.divSince.delete(symbol);
      this.resolve(k, `${symbol} prices agree again (${bps.toFixed(0)} bps).`);
    }
  }

  private expire(fs: FeedState, at: number) {
    const cutoff = at - THRESHOLDS.depegWindowS * 1000;
    while (fs.window.length && fs.window[0][0] < cutoff) fs.window.shift();
  }

  vwap(fs: FeedState): number | null {
    let n = 0, v = 0;
    for (const [, p, s] of fs.window) {
      n += p * s;
      v += s;
    }
    return v > 0 ? n / v : null;
  }

  private checkDepeg(symbol: string, at: number) {
    const devs = new Map<string, number>(), vwaps = new Map<string, number>();
    for (const [k, fs] of this.feeds) {
      const [source, sym] = k.split("|");
      if (sym !== symbol) continue;
      this.expire(fs, at);
      const vw = this.vwap(fs);
      if (vw !== null && fs.window.length >= THRESHOLDS.depegMinTrades) {
        vwaps.set(source, vw);
        devs.set(source, (vw - 1) * 100);
      }
    }
    if (!devs.size) return;
    const k = key("depeg", null, symbol);
    const smallest = Math.min(...[...devs.values()].map(Math.abs));
    const coin = symbol.split("-")[0];
    if (smallest >= THRESHOLDS.depegWarnPct) {
      this.pegOkSince.delete(symbol);
      const inc = this.open.get(k);
      let low = Math.min(...vwaps.values());
      if (inc && typeof inc.details.low === "number") low = Math.min(low, inc.details.low);
      const worst = [...devs].sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))[0][0];
      const critical = smallest >= THRESHOLDS.depegCriticalPct
        || (inc?.severity === "critical" && smallest >= DEESCALATE * THRESHOLDS.depegCriticalPct);
      const d = devs.get(worst) as number;
      this.upsert(k, "depeg", null, symbol, critical ? "critical" : "warning",
        `${coin} trading at $${(vwaps.get(worst) as number).toFixed(4)} (${d >= 0 ? "+" : ""}${d.toFixed(2)} %) on ${[...devs.keys()].sort().join(" and ")}`,
        { deviationPct: Object.fromEntries(devs), vwap: Object.fromEntries(vwaps), low, confirmed: devs.size >= 2 });
    } else if (this.open.has(k)) {
      if (Math.max(...[...devs.values()].map(Math.abs)) < THRESHOLDS.depegResolvePct) {
        if (!this.pegOkSince.has(symbol)) this.pegOkSince.set(symbol, at);
        if (at - (this.pegOkSince.get(symbol) as number) >= THRESHOLDS.depegResolveHoldS * 1000) {
          this.pegOkSince.delete(symbol);
          this.resolve(k, `${coin} back within ${THRESHOLDS.depegResolvePct} % of $1.`);
        }
      } else this.pegOkSince.delete(symbol);
    }
  }

  // ------------------------------------------------------------------ time rules
  private timeRules() {
    if (this.feedRules) {
      for (const [name, st] of this.sources) {
        const last = st.lastMessageMs ?? this.startedMs;
        if (last !== null && this.now - last > THRESHOLDS.silentAfterS * 1000 && !this.open.has(key("feed_silent", name, null))) {
          this.upsert(key("feed_silent", name, null), "feed_silent", name, null, "critical",
            `No messages from ${name} for over ${THRESHOLDS.silentAfterS} s`, { silentSinceMs: last, connected: st.connected });
        }
        const cutoff = this.now - THRESHOLDS.latencyWindowS * 1000;
        while (st.latency.length && st.latency[0][0] < cutoff) st.latency.shift();
        const values = st.latency.map(([, v]) => v);
        const p95 = values.length >= 20 ? percentile(values, 95) : null;
        const lk = key("high_latency", name, null);
        if (p95 !== null && p95 > THRESHOLDS.latencyP95Ms) {
          this.upsert(lk, "high_latency", name, null, "warning", `${name} data is arriving late: p95 ${(p95 / 1000).toFixed(1)} s`,
            { p95Ms: Math.round(p95), samples: values.length });
        } else if (p95 !== null && p95 < 0.7 * THRESHOLDS.latencyP95Ms) {
          this.resolve(lk, `${name} latency back to normal (p95 ${p95.toFixed(0)} ms).`);
        }
      }
    }
    for (const [k, inc] of [...this.open]) {
      if ((inc.type === "trade_gap" || inc.type === "outlier_print")
        && this.now - ((inc.details.lastHitMs as number) ?? inc.updatedMs) > BURST_QUIET_MS) {
        this.resolve(k, "No new occurrences for a minute.");
      }
    }
  }

  // ------------------------------------------------------------------ incidents
  private upsert(k: string, type: IncidentType, source: string | null, symbol: string | null, severity: Severity,
    summary: string, details: Record<string, unknown>, force = false) {
    const inc = this.open.get(k);
    if (!inc) {
      const created: Incident = { id: `${type}:${source ?? "*"}:${symbol ?? "*"}:${this.now}`, type, severity, source, symbol,
        openedMs: this.now, updatedMs: this.now, resolvedMs: null, summary, details };
      this.open.set(k, created);
      this.emit(created);
      return;
    }
    const changed = inc.severity !== severity;
    Object.assign(inc, { severity, summary, details, updatedMs: this.now });
    if (changed || force || this.now - (this.emitted.get(inc.id) ?? 0) >= REEMIT_MS) this.emit(inc);
  }

  private resolve(k: string, summary: string) {
    const inc = this.open.get(k);
    if (!inc) return;
    this.open.delete(k);
    inc.resolvedMs = inc.updatedMs = this.now;
    inc.details = { ...inc.details, resolution: summary };
    this.emit(inc);
  }

  private emit(inc: Incident) {
    this.emitted.set(inc.id, this.now);
    this.changes.push({ ...inc, details: { ...inc.details } });
  }

  // ------------------------------------------------------------------ health
  rate(times: number[], spanMs = 10_000): number {
    const cutoff = this.now - spanMs;
    while (times.length && times[0] < cutoff) times.shift();
    return times.length / (spanMs / 1000);
  }
}
