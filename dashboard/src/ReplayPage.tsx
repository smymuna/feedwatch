import {
  CandlestickSeries,
  ColorType,
  createChart,
  createSeriesMarkers,
  type IChartApi,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";
import { useEffect, useMemo, useRef, useState } from "react";

interface Change {
  id: string;
  type: string;
  severity: "info" | "warning" | "critical";
  state: "opened" | "changed" | "resolved";
  time: number;
  text: string;
}
interface ReplayData {
  trades: number;
  candles: [number, number, number, number, number, number, number][]; // t, o, h, l, c, v, n
  changes: Change[];
}

// What happened in the world, for context (UTC).
const WORLD = [
  { time: Date.UTC(2023, 2, 10, 16, 0) / 1000, text: "Silicon Valley Bank is closed by regulators (Friday)." },
  { time: Date.UTC(2023, 2, 11, 3, 0) / 1000, text: "Circle says $3.3B of USDC's reserves are held at SVB." },
  { time: Date.UTC(2023, 2, 12, 22, 15) / 1000, text: "US authorities announce all SVB deposits are protected (Sunday evening)." },
  { time: Date.UTC(2023, 2, 13, 13, 0) / 1000, text: "Banks reopen; USDC climbs back to $1 (Monday)." },
];
const SPEEDS = [
  { label: "1 h/s", secondsPerTick: 3600 / 10 },
  { label: "6 h/s", secondsPerTick: 21600 / 10 },
];
const COLOR = { warning: "#f2b441", critical: "#ff6363", info: "#7aa2ff" } as const;
const fmt = (t: number) =>
  new Date(t * 1000).toLocaleString("en-GB", { timeZone: "UTC", weekday: "short", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" }) + " UTC";

export function ReplayPage() {
  const [data, setData] = useState<ReplayData | null>(null);
  const [cursor, setCursor] = useState(0); // unix seconds
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(0);
  const box = useRef<HTMLDivElement>(null);
  const chart = useRef<{ api: IChartApi; series: ISeriesApi<"Candlestick">; markers: ISeriesMarkersPluginApi<Time> } | null>(null);

  useEffect(() => {
    void fetch(`${import.meta.env.BASE_URL}replay/usdc-2023.json`).then((r) => r.json()).then((d: ReplayData) => {
      setData(d);
      setCursor(d.candles[0][0] + 12 * 3600);
    });
  }, []);

  const start = data?.candles[0][0] ?? 0;
  const end = data?.candles.at(-1)?.[0] ?? 0;

  useEffect(() => {
    if (!box.current || !data) return;
    const api = createChart(box.current, {
      autoSize: true,
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#8d949e", fontSize: 11 },
      grid: { vertLines: { color: "#1b2027" }, horzLines: { color: "#1b2027" } },
      rightPriceScale: { borderColor: "#1b2027" },
      timeScale: { borderColor: "#1b2027", timeVisible: true, secondsVisible: false },
      localization: { timeFormatter: (t: Time) => fmt(t as number) },
    });
    const series = api.addSeries(CandlestickSeries, {
      upColor: "#3cc98e", downColor: "#ff6363", borderVisible: false, wickUpColor: "#3cc98e", wickDownColor: "#ff6363",
      priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
    });
    series.createPriceLine({ price: 0.995, color: "#f2b44199", lineWidth: 1, lineStyle: 2, title: "warning −0.5 %" });
    series.createPriceLine({ price: 0.98, color: "#ff636399", lineWidth: 1, lineStyle: 2, title: "critical −2 %" });
    chart.current = { api, series, markers: createSeriesMarkers(series, []) };
    return () => {
      api.remove();
      chart.current = null;
    };
  }, [data]);

  // Draw everything up to the cursor (15-minute candles keep three days readable).
  const visible = useMemo(() => {
    if (!data) return [];
    const out: { time: UTCTimestamp; open: number; high: number; low: number; close: number }[] = [];
    for (const [t, o, h, l, c] of data.candles) {
      if (t > cursor) break;
      const bucket = t - (t % 900);
      const last = out.at(-1);
      if (last && last.time === bucket) {
        last.high = Math.max(last.high, h);
        last.low = Math.min(last.low, l);
        last.close = c;
      } else out.push({ time: bucket as UTCTimestamp, open: o, high: h, low: l, close: c });
    }
    return out;
  }, [data, cursor]);
  const seen = useMemo(() => (data?.changes ?? []).filter((c) => c.time <= cursor), [data, cursor]);

  useEffect(() => {
    const c = chart.current;
    if (!c) return;
    c.series.setData(visible);
    c.markers.setMarkers(seen.map((ch) => ({
      time: (ch.time - (ch.time % 900)) as UTCTimestamp,
      position: ch.state === "resolved" ? "belowBar" : "aboveBar",
      shape: ch.state === "resolved" ? "circle" : "arrowDown",
      color: ch.state === "resolved" ? "#3cc98e" : COLOR[ch.severity],
      text: ch.state === "resolved" ? "resolved" : ch.severity,
    })));
  }, [visible, seen]);

  useEffect(() => {
    if (!playing || !data) return;
    const id = window.setInterval(() => {
      setCursor((c) => {
        const next = c + SPEEDS[speed].secondsPerTick;
        if (next >= end) {
          setPlaying(false);
          return end;
        }
        return next;
      });
    }, 100);
    return () => window.clearInterval(id);
  }, [playing, speed, data, end]);

  const last = visible.at(-1);
  const low = visible.reduce((m, c) => Math.min(m, c.low), Infinity);
  const state = seen.at(-1);
  const status = !state ? "On peg" : state.state === "resolved" ? "Resolved" : state.severity === "critical" ? "Critical depeg" : "Off peg (warning)";

  return (
    <div className="coin-page">
      <a className="back" href="#/">← Back</a>
      <div className="coin-head">
        <div>
          <h2 className="coin-title">USDC <span>the March 2023 depeg, replayed</span></h2>
          <p className="muted small">
            {data ? `${data.trades.toLocaleString()} real Kraken trades, 10–13 March 2023, through FeedWatch's detector` : "Loading…"}
          </p>
        </div>
      </div>

      <div className="price-strip">
        <div className="price-card"><span className="muted small">Replay time (UTC)</span><span className="big num" style={{ fontSize: "1.05rem" }}>{data ? fmt(cursor).replace(" UTC", "") : "–"}</span></div>
        <div className="price-card"><span className="muted small">USDC price</span><span className="big num">{last ? last.close.toFixed(4) : "–"}</span></div>
        <div className="price-card"><span className="muted small">Lowest so far</span><span className="big num">{Number.isFinite(low) ? low.toFixed(4) : "–"}</span></div>
        <div className="price-card"><span className="muted small">FeedWatch says</span>
          <span className={`big ${state?.state === "resolved" || !state ? "ok-text" : state.severity === "critical" ? "crit-text" : "warn"}`} style={{ fontSize: "1.2rem" }}>{status}</span></div>
      </div>

      <section className="panel" aria-label="Replay chart">
        <div className="chart-head">
          <div className="replay-controls">
            <button className="play" onClick={() => { if (cursor >= end) setCursor(start); setPlaying((p) => !p); }} aria-label={playing ? "Pause" : "Play"}>
              {playing ? "❚❚ Pause" : "▶ Play"}
            </button>
            <div className="seg" role="group" aria-label="Speed">
              {SPEEDS.map((s, i) => <button key={s.label} aria-pressed={i === speed} onClick={() => setSpeed(i)}>{s.label}</button>)}
            </div>
          </div>
          <span className="muted small">15-minute candles · arrows: FeedWatch alerts · dots: resolved</span>
        </div>
        <input className="scrubber" type="range" min={start} max={end} step={300} value={cursor} aria-label="Replay time"
          onChange={(e) => { setPlaying(false); setCursor(+e.target.value); }} />
        <div className="candle-chart" ref={box} role="img" aria-label="USDC price in March 2023" />
      </section>

      <div className="coin-grid lower">
        <section className="panel" aria-label="FeedWatch alerts">
          <h3>FeedWatch's alerts so far</h3>
          {seen.length === 0 ? <p className="muted small">None yet: USDC is on its peg. Press play.</p> : (
            <ol className="incident-list">
              {seen.slice().reverse().map((c) => (
                <li key={`${c.id}-${c.time}`} className={`incident ${c.state === "resolved" ? "resolved" : "open"} ${c.severity}`}>
                  <span className={`sev ${c.state === "resolved" ? "info" : c.severity}`} />
                  <div className="body">
                    <div className="line"><strong>{c.state === "opened" ? "Depeg opened" : c.state === "resolved" ? "Depeg resolved" : `Now ${c.severity}`}</strong><span className="muted small">{fmt(c.time)}</span></div>
                    <p>{c.text}</p>
                  </div>
                </li>
              ))}
            </ol>
          )}
        </section>
        <section className="panel" aria-label="What happened">
          <h3>Meanwhile, in the world</h3>
          <ol className="world-list">
            {WORLD.map((w) => (
              <li key={w.time} className={w.time <= cursor ? "past" : ""}><span className="muted small">{fmt(w.time)}</span><p>{w.text}</p></li>
            ))}
          </ol>
          <p className="muted small" style={{ marginTop: ".8rem" }}>
            FeedWatch only sees trades. It stepped down from critical 11 minutes after the Sunday-evening announcement, from the prices alone.
          </p>
        </section>
      </div>
    </div>
  );
}
