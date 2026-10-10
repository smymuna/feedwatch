import { useEffect, useState } from "react";
import { BROWSER, type Feed, type Incident, type SymbolHealth, useIncidents, useNow, useSnapshot } from "./api";
import { CoinPage } from "./CoinPage";
import { useCoins } from "./coins";
import { CoinCards, IncidentList } from "./components";
import { ReplayPage } from "./ReplayPage";
import { setTheme, useTheme } from "./theme";
import { ago, gaugePosition, pct, pegLevel, price, rate } from "./format";

/** Tiny hash router: #/ is the overview, #/coin/BTC-USD a coin page, #/replay the 2023 replay. */
function useRoute(): string | null {
  const read = () => {
    if (window.location.hash === "#/replay") return "replay";
    const m = window.location.hash.match(/^#\/coin\/([A-Z0-9]+-[A-Z]+)$/);
    return m ? m[1] : null;
  };
  const [coin, setCoin] = useState(read);
  useEffect(() => {
    const on = () => {
      setCoin(read());
      window.scrollTo({ top: 0 });
    };
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  return coin;
}

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
  const coin = useRoute();
  const coins = useCoins();
  useEffect(() => {
    document.title = coin === "replay" ? "USDC 2023 replay · FeedWatch" : coin ? `${coin} · FeedWatch` : "FeedWatch · market data quality";
  }, [coin]);

  return (
    <div className="app">
      <header className="top">
        <div>
          <h1><a href="#/" className="home">FeedWatch</a></h1>
          <p className="sub">Live data quality for crypto market data feeds · Coinbase and Kraken</p>
        </div>
        <div className="status-group">
          <StatusPill open={open.length} critical={critical} ready={snapshot !== null} />
          <span className={`stream ${state === "live" && !stale ? "on" : "off"}`}>
            <i /> {state === "live" && !stale ? "live" : state === "connecting" ? "connecting…" : "reconnecting…"}
          </span>
          <ThemeToggle />
        </div>
      </header>

      {BROWSER && (
        <aside className="edition">
          <strong>Browser edition.</strong> This page connects straight from your browser to Coinbase and Kraken and runs
          FeedWatch's detection rules here, live. The full system (Kafka, Quix Streams, TimescaleDB, FastAPI) is in{" "}
          <a href="https://github.com/smymuna/feedwatch">the repository</a> and runs with one command.
        </aside>
      )}
      {coin === "replay" ? (
        <ReplayPage />
      ) : coin ? (
        <CoinPage symbol={coin} now={now} />
      ) : !snapshot ? (
        <p className="empty">Waiting for the first snapshot from the API…</p>
      ) : (
        <main className="grid">
          <a className="panel replay-card" href="#/replay">
            <div>
              <strong>Replay a real incident: the USDC depeg of March 2023</strong>
              <p className="muted small">269,430 real trades through the same detector. Press play and watch FeedWatch raise and clear the alarm.</p>
            </div>
            <span className="replay-go">Open replay →</span>
          </a>

          <section className="panel coins" aria-labelledby="coins-h">
            <h2 id="coins-h">Coins <small>live price, last hour · click a coin for its live chart and trades</small></h2>
            <CoinCards coins={coins} />
          </section>

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
              <th scope="row"><a href={`#/coin/${sym}`}>{coin}</a></th>
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
              <th scope="row"><a href={`#/coin/${sym}`}>{sym}</a></th>
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

function ThemeToggle() {
  const theme = useTheme();
  const next = theme === "dark" ? "light" : "dark";
  return (
    <button className="theme-toggle" onClick={() => setTheme(next)} aria-label={`Switch to ${next} mode`} title={`Switch to ${next} mode`}>
      {theme === "dark" ? (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true">
          <circle cx="12" cy="12" r="4" />
          <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
        </svg>
      ) : (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d="M20 14.5A8 8 0 0 1 9.5 4 8 8 0 1 0 20 14.5Z" />
        </svg>
      )}
    </button>
  );
}
