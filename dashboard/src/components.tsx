import type { Incident } from "./api";
import type { CoinSummary } from "./coins";
import { TYPE_LABEL, ago, duration, pct, price } from "./format";

export function Sparkline({ values, up }: { values: number[]; up: boolean }) {
  if (values.length < 2) return <svg className="spark" aria-hidden="true" />;
  const lo = Math.min(...values), hi = Math.max(...values), span = hi - lo || 1;
  const pts = values.map((v, i) => `${(i / (values.length - 1)) * 100},${28 - ((v - lo) / span) * 26}`).join(" ");
  return (
    <svg className="spark" viewBox="0 0 100 30" preserveAspectRatio="none" aria-hidden="true">
      <polyline points={pts} fill="none" stroke={up ? "var(--ok)" : "var(--critical)"} strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

export function CoinCards({ coins }: { coins: CoinSummary[] | null }) {
  if (!coins) return <p className="muted">Loading coins…</p>;
  return (
    <div className="coin-cards">
      {coins.map((c) => {
        const main = c.prices.coinbase ?? c.prices[c.sources[0]] ?? Object.values(c.prices)[0];
        const change = c.change_60m_pct;
        const worst = c.open_incidents.includes("critical") ? "critical" : c.open_incidents.length ? "warning" : null;
        return (
          <a key={c.symbol} className="coin-card" href={`#/coin/${c.symbol}`} aria-label={`${c.name} ${c.symbol}: open details`}>
            <div className="coin-card-top">
              <strong>{c.base}</strong>
              <span className="muted small">{c.name}</span>
              {worst && <span className={`dot ${worst}`} title={`${c.open_incidents.length} open incident(s)`} />}
            </div>
            <div className="coin-price num">{main ? price(main.price) : c.sparkline.length ? price(String(c.sparkline[c.sparkline.length - 1])) : "–"}</div>
            <div className="coin-card-bottom">
              <span className={`num small ${change == null ? "muted" : change >= 0 ? "ok-text" : "crit-text"}`}>
                {change == null ? "no history yet" : `${pct(change, c.stablecoin ? 3 : 2)} 1 h`}
              </span>
              <Sparkline values={c.sparkline} up={(change ?? 0) >= 0} />
            </div>
          </a>
        );
      })}
    </div>
  );
}

export function IncidentList({ items, now, empty }: { items: Incident[]; now: number; empty?: string }) {
  if (!items.length) {
    return empty ? <p className="muted">{empty}</p> : (
      <p className="muted">No incidents yet. Run a fault scenario to see one: <code>make demo</code></p>
    );
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
