import { useEffect, useRef, useState } from "react";
import { engine } from "./browser/engine";

/** The browser edition (GitHub Pages) runs the pipeline in the page instead of calling the API. */
export const BROWSER = import.meta.env.VITE_BROWSER === "1";

export type Severity = "info" | "warning" | "critical";

export interface Feed {
  source: string;
  connected: boolean;
  last_message_at: string | null;
  last_trade_at: string | null;
  messages_per_s: number;
  trades_per_s: number;
  latency_p95_ms: number | null;
  gaps_total: number;
  missing_trades_total: number;
  updated_at: string;
}

export interface SymbolHealth {
  symbol: string;
  source: string;
  last_price: string | null;
  vwap_60s: string | null;
  trades_60s: number;
  peg_dev_pct: number | null;
  divergence_bps: number | null;
  last_trade_at: string | null;
  updated_at: string;
}

export interface Incident {
  id: string;
  type: string;
  severity: Severity;
  status: "open" | "resolved";
  source: string | null;
  symbol: string | null;
  opened_at: string;
  updated_at: string;
  resolved_at: string | null;
  summary: string;
  details: Record<string, unknown>;
}

export interface Snapshot {
  feeds: Feed[];
  symbols: SymbolHealth[];
  open_incidents: Incident[];
}

export type StreamState = "connecting" | "live" | "reconnecting";

/** Live snapshots over Server-Sent Events. EventSource reconnects by itself; we just
 *  report the state so the UI can say so honestly. */
function useSnapshotServer(): { snapshot: Snapshot | null; state: StreamState; receivedAt: number } {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [state, setState] = useState<StreamState>("connecting");
  const [receivedAt, setReceivedAt] = useState(0);
  useEffect(() => {
    const source = new EventSource("/api/stream");
    source.addEventListener("snapshot", (e) => {
      setSnapshot(JSON.parse((e as MessageEvent<string>).data) as Snapshot);
      setReceivedAt(Date.now());
      setState("live");
    });
    source.onerror = () => setState("reconnecting");
    return () => source.close();
  }, []);
  return { snapshot, state, receivedAt };
}

function useSnapshotBrowser(): { snapshot: Snapshot | null; state: StreamState; receivedAt: number } {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [state, setState] = useState<StreamState>("connecting");
  const [receivedAt, setReceivedAt] = useState(0);
  useEffect(() => {
    engine.start();
    const id = window.setInterval(() => {
      setSnapshot(engine.snapshot());
      setReceivedAt(Date.now());
      setState(Object.values(engine.connected).some(Boolean) ? "live" : "reconnecting");
    }, 1000);
    return () => window.clearInterval(id);
  }, []);
  return { snapshot, state, receivedAt };
}

export const useSnapshot = BROWSER ? useSnapshotBrowser : useSnapshotServer;

function useIncidentsBrowser(everyMs = 2000): Incident[] {
  const [items, setItems] = useState<Incident[]>([]);
  useEffect(() => {
    engine.start();
    setItems(engine.incidentList());
    const id = window.setInterval(() => setItems(engine.incidentList()), everyMs);
    return () => window.clearInterval(id);
  }, [everyMs]);
  return items;
}

export const useIncidents = BROWSER ? useIncidentsBrowser : useIncidentsServer;

/** Recent incidents (open and resolved), refreshed every few seconds. */
function useIncidentsServer(everyMs = 4000): Incident[] {
  const [items, setItems] = useState<Incident[]>([]);
  const timer = useRef<number | undefined>(undefined);
  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const res = await fetch("/api/incidents?status=all&limit=40");
        if (res.ok && alive) setItems((await res.json()) as Incident[]);
      } catch {
        /* the stream state already shows we're offline */
      }
      if (alive) timer.current = window.setTimeout(load, everyMs);
    };
    void load();
    return () => {
      alive = false;
      window.clearTimeout(timer.current);
    };
  }, [everyMs]);
  return items;
}

/** Re-render every `ms` so "3 s ago" labels keep counting. */
export function useNow(ms = 1000): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), ms);
    return () => window.clearInterval(id);
  }, [ms]);
  return now;
}
