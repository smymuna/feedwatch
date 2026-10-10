import { type Feed, type Incident, type SymbolHealth, useIncidents, useNow, useSnapshot } from "./api";
import { TYPE_LABEL, ago, duration, gaugePosition, pct, pegLevel, price, rate } from "./format";

const STABLES = ["USDT", "USDC", "DAI", "PYUSD", "USDS"];
const SOURCES = ["coinbase", "kraken"];
// Which stablecoin/USD pairs each exchange lists (see docs/data-sources.md).
const LISTED: Record<string, string[]> = {
  coinbase: ["USDT", "USDS"],
  kraken: ["USDT", "USDC", "DAI", "PYUSD", "USDS"],
};

export default function App() {
  const { snapshot, state, receivedAt } = useSnapshot();
  const incidents = useIncidents();
  const now = useNow();
  const open = snapshot?.open_incidents ?? [];
  const critical = open.filter((i) => i.severity === "critical").length;
  const stale = state === "live" && now - receivedAt > 5000;

  return (
    <div className="app">
      <header className="top">
        <div>
          <h1>FeedWatch</h1>
          <p className="sub">Live data quality for crypto market data feeds · Coinbase and Kraken</p>
        </div>
        <div className="status-group">
          <StatusPill open={open.length} critical={critical} ready={snapshot !== null} />
          <span className={`stream ${state === "live" && !stale ? "on" : "off"}`}>
            <i /> {state === "live" && !stale ? "live" : state === "connecting" ? "connecting…" : "reconnecting…"}
          </span>
        </div>
      </header>

      {!snapshot ? (
        <p className="empty">Waiting for the first snapshot from the API…</p>
      ) : (
        <main className="grid">
          <section className="panel feeds" aria-labelledby="feeds-h">
            <h2 id="feeds-h">Feeds</h2>
            <div className="feed-cards">
              {SOURCES.map((s) => (
                <FeedCard key={s} feed={snapshot.feeds.find((f) => f.source === s)} name={s} now={now} />
              ))}
            </div>
          </section>

          <section className="panel pegs" aria-labelledby="pegs-h">
            <h2 id="pegs-h">Stablecoin pegs <small>60 s volume-weighted price vs $1</small></h2>
            <PegBoard symbols={snapshot.symbols} open={open} />
          </section>

          <section className="panel agree" aria-labelledby="agree-h">
            <h2 id="agree-h">Do the exchanges agree? <small>difference between Coinbase and Kraken</small></h2>
            <Agreement symbols={snapshot.symbols} />
          </section>

          <section className="panel incidents" aria-labelledby="inc-h">
            <h2 id="inc-h">Incidents <small>open first, then the latest resolved</small></h2>
            <IncidentList items={incidents} now={now} />
          </section>
        </main>
      )}
      <footer className="foot">
        Read-only monitoring of public market data. It does not trade or give financial advice.
      </footer>
    </div>
  );
}

function StatusPill({ open, critical, ready }: { open: number; critical: number; ready: boolean }) {
  if (!ready) return <span className="pill none">No data yet</span>;
  if (open === 0) return <span className="pill ok">All feeds healthy</span>;
  return (
    <span className={`pill ${critical ? "critical" : "warning"}`}>
      {open} open incident{open > 1 ? "s" : ""}
      {critical ? ` · ${critical} critical` : ""}
    </span>
  );
}

function FeedCard({ feed, name, now }: { feed: Feed | undefined; name: string; now: number }) {
  if (!feed) {
    return (
      <div className="feed-card">
        <div className="feed-name"><i className="dot none" />{name}</div>
        <p className="muted">No data yet</p>
      </div>
    );
  }
  return (
    <div className="feed-card">
      <div className="feed-name">
        <i className={`dot ${feed.connected ? "ok" : "critical"}`} />
        {name}
        <span className="muted">{feed.connected ? "connected" : "silent"}</span>
      </div>
      <dl className="stats">
        <div><dt>Messages</dt><dd>{rate(feed.messages_per_s)}<small>/s</small></dd></div>
        <div><dt>Trades</dt><dd>{rate(feed.trades_per_s)}<small>/s</small></dd></div>
        <div><dt>Latency p95</dt><dd title="Exchange timestamp to arrival. A few ms below zero means our clock is slightly behind the exchange's.">{feed.latency_p95_ms === null ? "–" : feed.latency_p95_ms < 1 ? "<1" : feed.latency_p95_ms}<small>ms</small></dd></div>
        <div><dt>Missing trades</dt><dd className={feed.missing_trades_total ? "warn" : ""}>{feed.missing_trades_total.toLocaleString()}</dd></div>
      </dl>
      <p className="muted small">Last message {ago(feed.last_message_at, now)} · {feed.gaps_total} gap{feed.gaps_total === 1 ? "" : "s"} since start</p>
    </div>
  );
}

function PegBoard({ symbols, open }: { symbols: SymbolHealth[]; open: Incident[] }) {
  return (
    <table className="peg-table">
      <thead>
        <tr><th>Coin</th>{SOURCES.map((s) => <th key={s}>{s}</th>)}<th>Status</th></tr>
      </thead>
      <tbody>
        {STABLES.map((coin) => {
          const sym = `${coin}-USD`;
          const depeg = open.find((i) => i.type === "depeg" && i.symbol === sym);
          const hasData = symbols.some((s) => s.symbol === sym && s.peg_dev_pct !== null);
          return (
            <tr key={coin}>
              <th scope="row">{coin}</th>
              {SOURCES.map((src) => {
                const row = symbols.find((s) => s.symbol === sym && s.source === src);
                const listed = LISTED[src]?.includes(coin);
                return (
                  <td key={src}>
                    {row ? <Gauge row={row} /> : <span className="muted">{listed ? "no trades yet" : "not listed"}</span>}
                  </td>
                );
              })}
              <td>
                {depeg ? (
                  <span className={`badge ${depeg.severity}`}>{depeg.severity === "critical" ? "Depegged" : "Off peg"}</span>
                ) : hasData ? (
                  <span className="badge ok">On peg</span>
                ) : (
                  <span className="badge none">No data</span>
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function Gauge({ row }: { row: SymbolHealth }) {
  const dev = row.peg_dev_pct;
  const level = pegLevel(dev);
  return (
    <div className="gauge" title={`${row.trades_60s} trades in the last 60 s`}>
      <div className="track" aria-hidden="true">
        <span className="band" />
        <span className="center" />
        {dev !== null && <span className={`marker ${level}`} style={{ left: `${gaugePosition(dev)}%` }} />}
      </div>
      <span className={`dev ${level}`}>{dev === null ? "no recent trades" : `${price(row.vwap_60s)} (${pct(dev, 2)})`}</span>
    </div>
  );
}

function Agreement({ symbols }: { symbols: SymbolHealth[] }) {
  const both = [...new Set(symbols.map((s) => s.symbol))]
    .filter((sym) => SOURCES.every((src) => symbols.some((s) => s.symbol === sym && s.source === src)))
    .sort();
  if (!both.length) return <p className="muted">Waiting for both exchanges to trade the same coin.</p>;
  return (
    <table className="agree-table">
      <thead>
        <tr><th>Pair</th>{SOURCES.map((s) => <th key={s}>{s}</th>)}<th>Difference</th></tr>
      </thead>
      <tbody>
        {both.map((sym) => {
          const rows = SOURCES.map((src) => symbols.find((s) => s.symbol === sym && s.source === src));
          const bps = rows.find((r) => r?.divergence_bps != null)?.divergence_bps ?? null;
          const limit = STABLES.some((c) => sym.startsWith(`${c}-`)) ? 30 : 50;
          const level = bps === null ? "none" : bps > 3 * limit ? "critical" : bps > limit ? "warning" : "ok";
          return (
            <tr key={sym}>
              <th scope="row">{sym}</th>
              {rows.map((r, i) => <td key={SOURCES[i]} className="num">{price(r?.last_price ?? null)}</td>)}
              <td>
                {bps === null ? (
                  <span className="muted">one side idle</span>
                ) : (
                  <span className="bps">
                    <span className="bar"><span className={level} style={{ width: `${Math.min(100, (bps / (2 * limit)) * 100)}%` }} /></span>
                    <span className={`num ${level}`}>{bps.toFixed(1)} bps</span>
                  </span>
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function IncidentList({ items, now }: { items: Incident[]; now: number }) {
  if (!items.length) {
    return <p className="muted">No incidents yet. Run a fault scenario to see one: <code>feedwatch replay samples/session --scenario scenarios/drill.json</code></p>;
  }
  return (
    <ol className="incident-list">
      {items.map((inc) => (
        <li key={inc.id} className={`incident ${inc.status} ${inc.severity}`}>
          <span className={`sev ${inc.severity}`} aria-label={inc.severity} />
          <div className="body">
            <div className="line">
              <strong>{TYPE_LABEL[inc.type] ?? inc.type}</strong>
              <span className="muted">{[inc.source, inc.symbol].filter(Boolean).join(" · ")}</span>
              <span className={`state ${inc.status}`}>{inc.status === "open" ? "open" : "resolved"}</span>
            </div>
            <p>{inc.status === "open" ? inc.summary : String(inc.details.resolution ?? inc.summary)}</p>
            <p className="muted small">
              {inc.status === "open"
                ? `Open for ${duration(inc.opened_at, null, now)}`
                : `Lasted ${duration(inc.opened_at, inc.resolved_at, now)} · resolved ${ago(inc.resolved_at, now)}`}
            </p>
          </div>
        </li>
      ))}
    </ol>
  );
}
