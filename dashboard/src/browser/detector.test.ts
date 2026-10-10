import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import { Detector, type Event, type Incident, type Trade } from "./detector";

const T0 = 1_700_000_000_000;
const trade = (source: string, symbol: string, price: number, t: number, tradeId: number | null = null, size = 1): Trade => ({
  kind: "trade", source, symbol, tradeId, price, size, side: "buy", eventMs: T0 + t, ingestMs: T0 + t + 50,
});
const run = (d: Detector, events: Event[]) => events.flatMap((e) => d.process(e));
const of = (changes: Incident[], type: string) => changes.filter((c) => c.type === type);

describe("browser detector: same rules as the Python service", () => {
  it("counts missing trades from id jumps and merges contiguous heartbeat gaps", () => {
    const d = new Detector(["coinbase"]);
    const events: Event[] = [trade("coinbase", "BTC-USD", 1, 0, 100)];
    for (let s = 1; s < 9; s++) {
      events.push({ kind: "heartbeat", source: "coinbase", symbol: "BTC-USD", lastTradeId: 100 + 3 * s, eventMs: T0 + s * 1000, ingestMs: T0 + s * 1000 });
    }
    events.push(trade("coinbase", "BTC-USD", 1, 9500, 128));
    const gap = of(run(d, events), "trade_gap").at(-1);
    expect(gap?.details.gaps).toBe(1);
    expect(gap?.details.missingTrades).toBe(27);
  });

  it("flags a silent source and resolves it", () => {
    const d = new Detector(["coinbase", "kraken"]);
    const events: Event[] = [{ kind: "connection", source: "kraken", state: "subscribed", ingestMs: T0 }];
    for (let s = 1; s < 13; s++) events.push({ kind: "tick", ingestMs: T0 + s * 1000 });
    expect(new Set(of(run(d, events), "feed_silent").map((i) => i.source))).toEqual(new Set(["coinbase", "kraken"]));
    const back = run(d, [{ kind: "heartbeat", source: "kraken", symbol: null, lastTradeId: null, eventMs: null, ingestMs: T0 + 13_000 }]);
    expect(back.map((i) => [i.source, i.resolvedMs !== null])).toEqual([["kraken", true]]);
  });

  it("opens a divergence only after 10 s and resolves when prices agree", () => {
    const d = new Detector(["coinbase", "kraken"]);
    const events: Event[] = [];
    for (let s = 0; s < 15; s++) events.push(trade("coinbase", "BTC-USD", 60000, s * 1000), trade("kraken", "BTC-USD", 60600, s * 1000 + 10));
    const opened = of(run(d, events), "price_divergence");
    expect(opened[0].openedMs).toBeGreaterThanOrEqual(T0 + 10_000);
    const again: Event[] = [];
    for (let s = 15; s < 21; s++) again.push(trade("coinbase", "BTC-USD", 60000, s * 1000), trade("kraken", "BTC-USD", 60001, s * 1000 + 10));
    expect(of(run(d, again), "price_divergence").at(-1)?.resolvedMs).not.toBeNull();
  });

  it("reports a bad print the market ignored, not a real move", () => {
    const base = Array.from({ length: 30 }, (_, i) => trade("kraken", "ETH-USD", 2500 + (i % 3), i * 100, i));
    const d1 = new Detector(["kraken"]);
    const spike = [...base, trade("kraken", "ETH-USD", 2575, 3100, 30), trade("kraken", "ETH-USD", 2501, 3200, 31), trade("kraken", "ETH-USD", 2500, 3300, 32)];
    expect(of(run(d1, spike), "outlier_print")).toHaveLength(1);
    const d2 = new Detector(["kraken"]);
    const move = [...base, ...Array.from({ length: 10 }, (_, i) => trade("kraken", "ETH-USD", 2425, 3100 + i * 100, 30 + i))];
    expect(of(run(d2, move), "outlier_print")).toHaveLength(0);
  });

  it("replays the real March 2023 USDC depeg exactly like the Python detector", () => {
    const csv = gunzipSync(readFileSync(new URL("../../../samples/history/kraken_USDCUSD_2023-03-10_2023-03-13.csv.gz", import.meta.url))).toString();
    const d = new Detector(["kraken"], { feedRules: false });
    const latest = new Map<string, Incident>();
    let nextTick: number | null = null;
    for (const line of csv.split("\n").slice(1)) {
      if (!line) continue;
      const [id, t, price, volume, side] = line.split(",");
      const ms = +t;
      nextTick ??= ms + 1000;
      while (ms >= nextTick) {
        for (const c of d.process({ kind: "tick", ingestMs: nextTick })) latest.set(c.id, c);
        nextTick += 1000;
      }
      const ev: Trade = { kind: "trade", source: "kraken", symbol: "USDC-USD", tradeId: +id, price: +price, size: +volume, side, eventMs: ms, ingestMs: ms };
      for (const c of d.process(ev)) latest.set(c.id, c);
    }
    const incidents = [...latest.values()];
    expect(incidents.map((i) => i.type)).toEqual(["depeg", "depeg"]);
    const [early, main] = incidents;
    expect(new Date(early.openedMs).toISOString().slice(0, 16)).toBe("2023-03-10T22:02"); // same as Python
    expect(new Date(main.resolvedMs ?? 0).toISOString().slice(0, 16)).toBe("2023-03-13T13:53");
    expect(main.details.low as number).toBeLessThan(0.9);
  }, 60_000);
});
