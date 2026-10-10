import { useEffect, useMemo, useRef, useState } from "react";
import { CandleChart } from "./CandleChart";
import { mergeCandles, useCoin, useCoinStream, useStoredCandles } from "./coins";
import { IncidentList } from "./components";
import { ago, pct, price } from "./format";

export function CoinPage({ symbol, now }: { symbol: string; now: number }) {
  const detail = useCoin(symbol);
  const stored = useStoredCandles(symbol);
  const stream = useCoinStream(symbol);
  const sources = detail?.sources ?? [];
  const [source, setSource] = useState<string | null>(null);
  const active = source && sources.includes(source) ? source : sources[0] ?? "coinbase";
  const otherSource = sources.find((s) => s !== active);

  const live = stream.candles.length ? stream.candles : stored?.live ?? [];
  const candles = useMemo(() => mergeCandles(stored?.stored ?? [], live, active), [stored, live, active]);
  const other = useMemo(
    () => (otherSource ? mergeCandles(stored?.stored ?? [], live, otherSource) : []),
    [stored, live, otherSource],
  );
  const health = stream.health ?? detail?.health ?? [];
  const stable = detail?.stablecoin ?? false;
  const prices = stream.prices;
  const divergence = health.find((h) => h.divergence_bps !== null)?.divergence_bps ?? null;
  const open = (detail?.incidents ?? []).filter((i) => i.status === "open");
  const forming = candles.at(-1);

  return (
    <div className="coin-page">
      <a className="back" href="#/">← All coins</a>
      <div className="coin-head">
        <div>
          <h2 className="coin-title">
            {detail?.base ?? symbol.split("-")[0]} <span>{detail?.name}</span>
          </h2>
          <p className="muted small">
            {symbol} · {sources.length ? `on ${sources.join(" and ")}` : "loading…"}
            {stable ? " · stablecoin, should trade at $1" : ""}
          </p>
        </div>
        <span className={`stream ${stream.connected && stream.live ? "on" : "off"}`}>
          <i /> {stream.connected ? (stream.live ? "live trades" : "waiting for Kafka…") : "connecting…"}
        </span>
      </div>

      <div className="price-strip">
        {sources.map((src) => (
          <LivePriceCard key={src} source={src} value={prices[src]?.price ?? null}
            at={prices[src]?.time_ms ?? null} now={now} stable={stable} />
        ))}
        <div className="price-card">
          <span className="muted small">Exchanges differ by</span>
          <span className="big num">{divergence === null ? "–" : `${divergence.toFixed(1)} bps`}</span>
          <span className="muted small">{divergence === null ? "needs recent trades on both" : `limit ${stable ? 30 : 50} bps`}</span>
        </div>
        <div className="price-card">
          <span className="muted small">Open incidents</span>
          <span className={`big ${open.length ? "warn" : "ok-text"}`}>{open.length}</span>
          <span className="muted small">{open.length ? open.map((i) => i.type.replace("_", " ")).join(", ") : "data looks healthy"}</span>
        </div>
      </div>

      <div className="coin-grid">
        <section className="panel chart-panel" aria-label="Price chart">
          <div className="chart-head">
            <h3>1-minute candles <small className="muted">last 2 hours · newest candle updates live</small></h3>
            <div className="seg" role="group" aria-label="Exchange">
              {sources.map((s) => (
                <button key={s} aria-pressed={s === active} onClick={() => setSource(s)}>{s}</button>
              ))}
            </div>
          </div>
          {candles.length ? (
            <CandleChart candles={candles} other={other} otherLabel={otherSource ?? ""}
              incidents={detail?.incidents ?? []} stablecoin={stable} />
          ) : (
            <p className="empty">No trades for {symbol} on {active} yet. Thin markets can be quiet for minutes.</p>
          )}
          {forming && (
            <p className="muted small legend">
              Now: O {price(String(forming.open))} · H {price(String(forming.high))} · L {price(String(forming.low))} ·
              C {price(String(forming.close))} · {forming.trades} trade{forming.trades === 1 ? "" : "s"}
              {otherSource ? <> · <span className="other-key">– – {otherSource}</span> close</> : null}
            </p>
          )}
        </section>

        <section className="panel trades-panel" aria-label="Live trades">
          <h3>Live trades</h3>
          <TradeList trades={stream.trades} stable={stable} />
        </section>
      </div>

      <div className="coin-grid lower">
        <section className="panel" aria-label="Per exchange">
          <h3>Per exchange</h3>
          <table>
            <thead><tr><th /><th>Last price</th><th>60 s avg</th><th>Trades (60 s)</th>{stable && <th>vs $1</th>}<th>Delay p95</th><th>Last trade</th></tr></thead>
            <tbody>
              {sources.map((src) => {
                const h = health.find((r) => r.source === src);
                const m = detail?.latest_minute.find((r) => r.source === src);
                return (
                  <tr key={src}>
                    <th scope="row" className="cap">{src}</th>
                    <td className="num">{price(h?.last_price ?? null)}</td>
                    <td className="num">{price(h?.vwap_60s ?? null)}</td>
                    <td className="num">{h?.trades_60s ?? 0}</td>
                    {stable && <td className="num">{pct(h?.peg_dev_pct ?? null, 3)}</td>}
                    <td className="num">{m?.latency_p95_ms == null ? "–" : m.latency_p95_ms < 1 ? "<1 ms" : `${m.latency_p95_ms} ms`}</td>
                    <td className="muted">{ago(h?.last_trade_at ?? null, now)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </section>
        <section className="panel" aria-label="Incidents">
          <h3>Incidents for {detail?.base ?? symbol}</h3>
          <IncidentList items={detail?.incidents ?? []} now={now} empty={`No incidents for ${symbol} so far.`} />
        </section>
      </div>
    </div>
  );
}

function LivePriceCard({ source, value, at, now, stable }: {
  source: string; value: string | null; at: number | null; now: number; stable: boolean;
}) {
  const last = useRef<string | null>(null);
  const [flash, setFlash] = useState<"" | "up" | "down">("");
  useEffect(() => {
    if (value && last.current && value !== last.current) {
      setFlash(+value > +last.current ? "up" : "down");
      const id = window.setTimeout(() => setFlash(""), 600);
      last.current = value;
      return () => window.clearTimeout(id);
    }
    last.current = value;
  }, [value]);
  const dev = stable && value ? (+value - 1) * 100 : null;
  return (
    <div className="price-card">
      <span className="muted small cap">{source}</span>
      <span className={`big num flash-${flash}`}>{value ? price(value) : "–"}</span>
      <span className="muted small">
        {dev !== null ? `${pct(dev, 3)} from $1 · ` : ""}
        {at ? `last trade ${ago(new Date(at).toISOString(), now)}` : "no trades yet"}
      </span>
    </div>
  );
}

function TradeList({ trades, stable }: { trades: { seq: number; source: string; price: string; size: string; side: string; time_ms: number }[]; stable: boolean }) {
  if (!trades.length) return <p className="muted small">Waiting for the next trade…</p>;
  return (
    <ol className="trade-list">
      {trades.map((t) => (
        <li key={t.seq} className={t.side}>
          <span className="num">{new Date(t.time_ms).toLocaleTimeString([], { hour12: false })}</span>
          <span className="cap muted">{t.source}</span>
          <span className={`num side-${t.side}`}>{price(t.price)}</span>
          <span className="num muted">{(+t.size).toLocaleString("en-US", { maximumFractionDigits: stable ? 2 : 8 })}</span>
        </li>
      ))}
    </ol>
  );
}
