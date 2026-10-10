import { useEffect, useState } from "react";
import type { Incident, SymbolHealth } from "./api";

export interface LivePrice {
  price: string;
  time_ms: number;
}

export interface CoinSummary {
  symbol: string;
  base: string;
  name: string;
  stablecoin: boolean;
  sources: string[];
  prices: Record<string, LivePrice>;
  change_60m_pct: number | null;
  sparkline: number[];
  sparkline_source: string;
  open_incidents: string[];
}

export interface MinuteRow {
  bucket: string;
  source: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  trades: number;
  vwap: string;
  latency_p50_ms: number | null;
  latency_p95_ms: number | null;
}

export interface LiveCandle {
  minute_ms: number;
  source: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  trades: number;
}

export interface LiveTrade {
  seq: number;
  source: string;
  price: string;
  size: string;
  side: string;
  time_ms: number;
  trade_id: number | null;
}

export interface CoinDetail {
  symbol: string;
  base: string;
  name: string;
  stablecoin: boolean;
  sources: string[];
  health: SymbolHealth[];
  latest_minute: MinuteRow[];
  incidents: Incident[];
}

/** One candle as the chart wants it: seconds since epoch, numbers. */
export interface Candle {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  trades: number;
  live: boolean;
}

function usePolled<T>(url: string | null, everyMs: number): T | null {
  const [data, setData] = useState<T | null>(null);
  useEffect(() => {
    if (!url) return;
    let alive = true;
    let timer: number | undefined;
    setData(null);
    const load = async () => {
      try {
        const res = await fetch(url);
        if (res.ok && alive) setData((await res.json()) as T);
      } catch {
        /* keep the last good data; the page shows the stream state */
      }
      if (alive) timer = window.setTimeout(load, everyMs);
    };
    void load();
    return () => {
      alive = false;
      window.clearTimeout(timer);
    };
  }, [url, everyMs]);
  return data;
}

export const useCoins = () => usePolled<CoinSummary[]>("/api/coins", 5000);
export const useCoin = (symbol: string) => usePolled<CoinDetail>(`/api/coins/${symbol}`, 10000);
export const useStoredCandles = (symbol: string) =>
  usePolled<{ stored: MinuteRow[]; live: LiveCandle[] }>(`/api/coins/${symbol}/candles?minutes=120`, 30000);

export interface CoinStream {
  trades: LiveTrade[];
  candles: LiveCandle[];
  prices: Record<string, LivePrice>;
  health: SymbolHealth[] | null;
  live: boolean;
  connected: boolean;
}

/** Trades, forming candles and prices for one coin, pushed twice a second. */
export function useCoinStream(symbol: string, keepTrades = 80): CoinStream {
  const [state, setState] = useState<CoinStream>({
    trades: [], candles: [], prices: {}, health: null, live: false, connected: false,
  });
  useEffect(() => {
    setState({ trades: [], candles: [], prices: {}, health: null, live: false, connected: false });
    const source = new EventSource(`/api/coins/${symbol}/stream`);
    source.addEventListener("update", (e) => {
      const msg = JSON.parse((e as MessageEvent<string>).data) as Omit<CoinStream, "connected" | "health"> & {
        health?: SymbolHealth[];
      };
      setState((prev) => ({
        trades: [...msg.trades.slice().reverse(), ...prev.trades].slice(0, keepTrades),
        candles: msg.candles,
        prices: msg.prices,
        health: msg.health ?? prev.health,
        live: msg.live,
        connected: true,
      }));
    });
    source.onerror = () => setState((prev) => ({ ...prev, connected: false }));
    return () => source.close();
  }, [symbol, keepTrades]);
  return state;
}

/** Stored minutes are authoritative; live candles fill in the newest minutes, including
 *  the one still forming. */
export function mergeCandles(stored: MinuteRow[], live: LiveCandle[], source: string): Candle[] {
  const byTime = new Map<number, Candle>();
  for (const r of stored) {
    if (r.source !== source) continue;
    const time = Math.floor(Date.parse(r.bucket) / 1000);
    byTime.set(time, {
      time, open: +r.open, high: +r.high, low: +r.low, close: +r.close,
      volume: +r.volume, trades: r.trades, live: false,
    });
  }
  for (const c of live) {
    if (c.source !== source) continue;
    const time = Math.floor(c.minute_ms / 1000);
    if (byTime.has(time) && !byTime.get(time)?.live) continue;
    byTime.set(time, {
      time, open: +c.open, high: +c.high, low: +c.low, close: +c.close,
      volume: +c.volume, trades: c.trades, live: true,
    });
  }
  return [...byTime.values()].sort((a, b) => a.time - b.time);
}
