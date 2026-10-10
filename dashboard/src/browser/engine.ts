/**
 * The browser edition's pipeline: WebSockets to Coinbase and Kraken, the ported detector,
 * live candles and trades, and two hours of history from the exchanges' public REST APIs.
 * It produces the same data shapes as the FastAPI service, so the dashboard code is shared.
 */
import type { Feed, Incident as ApiIncident, Snapshot, SymbolHealth } from "../api";
import type { CoinDetail, CoinSummary, LiveCandle, LivePrice, LiveTrade, MinuteRow } from "../coins";
import { Detector, type Event, type Incident, isStable, percentile } from "./detector";

export const COINBASE_PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD", "USDT-USD", "USDS-USD"];
export const KRAKEN_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD", "USDT/USD", "USDC/USD", "DAI/USD", "PYUSD/USD", "USDS/USD"];
const KRAKEN_REST_PAIR: Record<string, string> = {
  "BTC-USD": "XBTUSD", "ETH-USD": "ETHUSD", "SOL-USD": "SOLUSD", "USDT-USD": "USDTUSD",
  "USDC-USD": "USDCUSD", "DAI-USD": "DAIUSD", "PYUSD-USD": "PYUSDUSD", "USDS-USD": "USDSUSD",
};
export const NAMES: Record<string, string> = {
  BTC: "Bitcoin", ETH: "Ether", SOL: "Solana", USDT: "Tether", USDC: "USD Coin", DAI: "Dai", PYUSD: "PayPal USD", USDS: "Sky Dollar",
};
const MINUTE = 60_000;
const HISTORY_MINUTES = 120;
const iso = (ms: number | null) => (ms === null ? null : new Date(ms).toISOString());
const taker: Record<string, string> = { buy: "sell", sell: "buy" };

export function coinSources(): Record<string, string[]> {
  const out: Record<string, string[]> = {};
  for (const p of COINBASE_PRODUCTS) (out[p] ??= []).push("coinbase");
  for (const s of KRAKEN_SYMBOLS) (out[s.replace("/", "-")] ??= []).push("kraken");
  return Object.fromEntries(Object.entries(out).sort(([a], [b]) => Number(isStable(a)) - Number(isStable(b)) || a.localeCompare(b)));
}

/** Coinbase and Kraken messages into events (the same rules as sources/*.py). */
export function parseCoinbase(msg: Record<string, unknown>, now: number): Event[] {
  const t = msg.type;
  if (t === "match" || t === "last_match") {
    return [{ kind: "trade", source: "coinbase", symbol: String(msg.product_id), tradeId: Number(msg.trade_id), price: Number(msg.price),
      size: Number(msg.size), side: taker[String(msg.side)] ?? String(msg.side), eventMs: Date.parse(String(msg.time)), ingestMs: now,
      snapshot: t === "last_match" }];
  }
  if (t === "heartbeat") {
    return [{ kind: "heartbeat", source: "coinbase", symbol: String(msg.product_id), lastTradeId: Number(msg.last_trade_id),
      eventMs: Date.parse(String(msg.time)), ingestMs: now }];
  }
  if (t === "subscriptions") return [{ kind: "connection", source: "coinbase", state: "subscribed", ingestMs: now }];
  return [];
}

export function parseKraken(msg: Record<string, unknown>, now: number): Event[] {
  if (msg.channel === "trade" && (msg.type === "update" || msg.type === "snapshot")) {
    return (msg.data as Record<string, unknown>[]).map((d) => ({
      kind: "trade" as const, source: "kraken", symbol: String(d.symbol).replace("/", "-"), tradeId: Number(d.trade_id), price: Number(d.price),
      size: Number(d.qty), side: String(d.side), eventMs: Date.parse(String(d.timestamp)), ingestMs: now, snapshot: msg.type === "snapshot",
    }));
  }
  if (msg.channel === "heartbeat") return [{ kind: "heartbeat", source: "kraken", symbol: null, lastTradeId: null, eventMs: null, ingestMs: now }];
  if (msg.channel === "status") return [{ kind: "connection", source: "kraken", state: "connected", ingestMs: now }];
  if (msg.method === "subscribe" && msg.success) return [{ kind: "connection", source: "kraken", state: "subscribed", ingestMs: now }];
  return [];
}

interface Candle extends LiveCandle {
  latencies: number[];
}

export class Engine {
  detector = new Detector(["coinbase", "kraken"]);
  private incidents = new Map<string, Incident>();
  private trades = new Map<string, LiveTrade[]>();
  private candles = new Map<string, Map<number, Candle>>(); // "symbol|source" -> minute -> candle
  private history = new Map<string, Promise<MinuteRow[]>>();
  private krakenQueue: Promise<unknown> = Promise.resolve();
  private seq = 0;
  private sockets: WebSocket[] = [];
  connected: Record<string, boolean> = { coinbase: false, kraken: false };
  private started = false;

  start() {
    if (this.started) return;
    this.started = true;
    this.connect("coinbase", "wss://ws-feed.exchange.coinbase.com",
      [{ type: "subscribe", product_ids: COINBASE_PRODUCTS, channels: ["matches", "heartbeat"] }], parseCoinbase);
    this.connect("kraken", "wss://ws.kraken.com/v2",
      [{ method: "subscribe", params: { channel: "trade", symbol: KRAKEN_SYMBOLS, snapshot: false } }], parseKraken);
    window.setInterval(() => this.feed({ kind: "tick", ingestMs: Date.now() }), 1000);
  }

  private connect(name: string, url: string, subs: unknown[], parse: (m: Record<string, unknown>, now: number) => Event[], backoff = 1000) {
    const ws = new WebSocket(url);
    this.sockets.push(ws);
    ws.onopen = () => {
      this.connected[name] = true;
      this.feed({ kind: "connection", source: name, state: "connected", ingestMs: Date.now() });
      for (const s of subs) ws.send(JSON.stringify(s));
      backoff = 1000;
    };
    ws.onmessage = (e) => {
      const now = Date.now();
      try {
        for (const ev of parse(JSON.parse(String(e.data)), now)) this.feed(ev);
      } catch {
        /* a malformed message: the full service sends these to a dead-letter topic */
      }
    };
    ws.onclose = () => {
      this.connected[name] = false;
      this.feed({ kind: "connection", source: name, state: "disconnected", ingestMs: Date.now() });
      const delay = Math.min(60_000, backoff) * (0.5 + Math.random());
      window.setTimeout(() => this.connect(name, url, subs, parse, backoff * 2), delay);
    };
  }

  feed(ev: Event) {
    for (const inc of this.detector.process(ev)) {
      this.incidents.set(inc.id, inc);
      if (this.incidents.size > 300) this.incidents.delete(this.incidents.keys().next().value as string);
    }
    if (ev.kind !== "trade" || ev.snapshot) return;
    const list = this.trades.get(ev.symbol) ?? [];
    list.push({ seq: ++this.seq, source: ev.source, price: String(ev.price), size: String(ev.size), side: ev.side, time_ms: ev.eventMs, trade_id: ev.tradeId });
    if (list.length > 300) list.shift();
    this.trades.set(ev.symbol, list);
    const k = `${ev.symbol}|${ev.source}`;
    const byMinute = this.candles.get(k) ?? new Map<number, Candle>();
    const minute = ev.eventMs - (ev.eventMs % MINUTE);
    const c = byMinute.get(minute);
    const p = String(ev.price);
    if (!c) {
      byMinute.set(minute, { minute_ms: minute, source: ev.source, open: p, high: p, low: p, close: p, volume: String(ev.size), trades: 1, latencies: [ev.ingestMs - ev.eventMs] });
    } else {
      if (ev.price > +c.high) c.high = p;
      if (ev.price < +c.low) c.low = p;
      c.close = p;
      c.volume = String(+c.volume + ev.size);
      c.trades += 1;
      if (c.latencies.length < 500) c.latencies.push(ev.ingestMs - ev.eventMs);
    }
    for (const m of byMinute.keys()) if (m < minute - HISTORY_MINUTES * MINUTE) byMinute.delete(m);
    this.candles.set(k, byMinute);
  }

  // ------------------------------------------------------------------ history (REST)
  loadHistory(symbol: string): Promise<MinuteRow[]> {
    let p = this.history.get(symbol);
    if (!p) {
      const sources = coinSources()[symbol] ?? [];
      const cutoff = Date.now() - HISTORY_MINUTES * MINUTE;
      const nowMinute = Date.now() - (Date.now() % MINUTE);
      const keep = (r: MinuteRow) => Date.parse(r.bucket) >= cutoff && Date.parse(r.bucket) < nowMinute;
      const jobs = sources.map((s) => (s === "coinbase" ? this.coinbaseHistory(symbol) : this.krakenHistory(symbol)).catch(() => []));
      p = Promise.all(jobs).then((rows) => rows.flat().filter(keep));
      this.history.set(symbol, p);
    }
    return p;
  }

  private async coinbaseHistory(symbol: string): Promise<MinuteRow[]> {
    const res = await fetch(`https://api.exchange.coinbase.com/products/${symbol}/candles?granularity=60`);
    const rows = (await res.json()) as number[][]; // [time, low, high, open, close, volume], newest first
    return rows.map(([t, low, high, open, close, volume]) => row(t * 1000, "coinbase", open, high, low, close, volume, null));
  }

  private krakenHistory(symbol: string): Promise<MinuteRow[]> {
    // Kraken's public API allows about one call a second: queue them.
    const job = this.krakenQueue.then(async () => {
      const res = await fetch(`https://api.kraken.com/0/public/OHLC?pair=${KRAKEN_REST_PAIR[symbol]}&interval=1`);
      const body = (await res.json()) as { result?: Record<string, unknown> };
      const rows = Object.entries(body.result ?? {}).find(([k]) => k !== "last")?.[1] as (string | number)[][] | undefined;
      await new Promise((r) => setTimeout(r, 1100));
      return (rows ?? []).map(([t, o, h, l, c, , v, count]) => row(+t * 1000, "kraken", +o, +h, +l, +c, +v, +count));
    });
    this.krakenQueue = job.catch(() => undefined);
    return job;
  }

  // ------------------------------------------------------------------ views (API shapes)
  private apiIncident(i: Incident): ApiIncident {
    return { id: i.id, type: i.type, severity: i.severity, status: i.resolvedMs === null ? "open" : "resolved", source: i.source, symbol: i.symbol,
      opened_at: iso(i.openedMs) as string, updated_at: iso(i.updatedMs) as string, resolved_at: iso(i.resolvedMs), summary: i.summary, details: i.details };
  }

  incidentList(symbol?: string, limit = 40): ApiIncident[] {
    return [...this.incidents.values()]
      .filter((i) => !symbol || i.symbol === symbol)
      .sort((a, b) => Number(b.resolvedMs === null) - Number(a.resolvedMs === null) || b.updatedMs - a.updatedMs)
      .slice(0, limit)
      .map((i) => this.apiIncident(i));
  }

  feeds(): Feed[] {
    const d = this.detector;
    return ["coinbase", "kraken"].map((name) => {
      const st = d.sources.get(name);
      const lat = (st?.latency ?? []).map(([, v]) => v);
      const silent = d.open.has(`feed_silent|${name}|*`);
      return {
        source: name, connected: !!st?.connected && this.connected[name] && !silent,
        last_message_at: iso(st?.lastMessageMs ?? null), last_trade_at: iso(st?.lastTradeMs ?? null),
        messages_per_s: st ? Math.round(d.rate(st.messages) * 100) / 100 : 0,
        trades_per_s: st ? Math.round(d.rate(st.trades) * 100) / 100 : 0,
        latency_p95_ms: (() => { const p = percentile(lat, 95); return p === null ? null : Math.round(p); })(),
        gaps_total: st?.gapsTotal ?? 0, missing_trades_total: st?.missingTotal ?? 0, updated_at: new Date().toISOString(),
      };
    });
  }

  symbols(only?: string): SymbolHealth[] {
    const d = this.detector;
    const out: SymbolHealth[] = [];
    for (const [k, fs] of d.feeds) {
      const [source, symbol] = k.split("|");
      if ((only && symbol !== only) || fs.lastEventMs === null) continue;
      const cutoff = Date.now() - 60_000;
      while (fs.window.length && fs.window[0][0] < cutoff) fs.window.shift();
      const vwap = d.vwap(fs);
      out.push({
        symbol, source, last_price: fs.lastPrice === null ? null : String(fs.lastPrice), vwap_60s: vwap === null ? null : String(vwap),
        trades_60s: fs.window.length, peg_dev_pct: vwap !== null && isStable(symbol) ? (vwap - 1) * 100 : null,
        divergence_bps: d.divBps.has(symbol) ? Math.round((d.divBps.get(symbol) as number) * 100) / 100 : null,
        last_trade_at: iso(fs.lastEventMs), updated_at: new Date().toISOString(),
      });
    }
    return out.sort((a, b) => a.symbol.localeCompare(b.symbol) || a.source.localeCompare(b.source));
  }

  snapshot(): Snapshot {
    return { feeds: this.feeds(), symbols: this.symbols(), open_incidents: this.incidentList().filter((i) => i.status === "open") };
  }

  prices(symbol: string): Record<string, LivePrice> {
    const out: Record<string, LivePrice> = {};
    for (const t of this.trades.get(symbol) ?? []) out[t.source] = { price: t.price, time_ms: t.time_ms };
    return out;
  }

  liveCandles(symbol: string): LiveCandle[] {
    const out: LiveCandle[] = [];
    for (const [k, byMinute] of this.candles) {
      if (k.split("|")[0] !== symbol) continue;
      for (const { latencies: _l, ...c } of byMinute.values()) out.push({ ...c });
    }
    return out.sort((a, b) => a.minute_ms - b.minute_ms);
  }

  tradesSince(symbol: string, seq: number): LiveTrade[] {
    return (this.trades.get(symbol) ?? []).filter((t) => t.seq > seq);
  }

  async coins(): Promise<CoinSummary[]> {
    const out: CoinSummary[] = [];
    for (const [symbol, sources] of Object.entries(coinSources())) {
      const history = this.history.has(symbol) ? await this.loadHistory(symbol) : [];
      const live = this.liveCandles(symbol);
      const main = sources[0];
      const series = mergeSeries(history, live, main);
      const change = series.length >= 2 && series[0].open > 0 ? (series.at(-1)!.close / series[0].open - 1) * 100 : null;
      const base = symbol.split("-")[0];
      out.push({
        symbol, base, name: NAMES[base] ?? "", stablecoin: isStable(symbol), sources, prices: this.prices(symbol),
        change_60m_pct: change, sparkline: series.map((c) => c.close), sparkline_source: main,
        open_incidents: this.incidentList(symbol).filter((i) => i.status === "open").map((i) => i.severity),
      });
    }
    return out;
  }

  coinDetail(symbol: string): CoinDetail {
    const base = symbol.split("-")[0];
    const latest: MinuteRow[] = [];
    for (const source of coinSources()[symbol] ?? []) {
      const minutes = [...(this.candles.get(`${symbol}|${source}`)?.values() ?? [])];
      const last = minutes.at(-1);
      if (last) latest.push(row(last.minute_ms, source, +last.open, +last.high, +last.low, +last.close, +last.volume, last.trades, percentile(last.latencies, 50), percentile(last.latencies, 95)));
    }
    return { symbol, base, name: NAMES[base] ?? "", stablecoin: isStable(symbol), sources: coinSources()[symbol] ?? [],
      health: this.symbols(symbol), latest_minute: latest, incidents: this.incidentList(symbol, 30) };
  }
}

function row(ms: number, source: string, open: number, high: number, low: number, close: number, volume: number,
  trades: number | null, p50: number | null = null, p95: number | null = null): MinuteRow {
  return { bucket: new Date(ms).toISOString(), source, open: String(open), high: String(high), low: String(low), close: String(close),
    volume: String(volume), trades: trades ?? 0, vwap: String(close), latency_p50_ms: p50 === null ? null : Math.round(p50),
    latency_p95_ms: p95 === null ? null : Math.round(p95) };
}

/** The last hour of closes for a coin card: history first, then live minutes. */
function mergeSeries(history: MinuteRow[], live: LiveCandle[], source: string) {
  const byTime = new Map<number, { open: number; close: number }>();
  const cutoff = Date.now() - 60 * MINUTE;
  for (const r of history) if (r.source === source && Date.parse(r.bucket) >= cutoff) byTime.set(Date.parse(r.bucket), { open: +r.open, close: +r.close });
  for (const c of live) if (c.source === source && !byTime.has(c.minute_ms)) byTime.set(c.minute_ms, { open: +c.open, close: +c.close });
  return [...byTime.entries()].sort(([a], [b]) => a - b).map(([, v]) => v);
}

export const engine = new Engine();
