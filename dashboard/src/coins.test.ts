import { describe, expect, it } from "vitest";
import { type LiveCandle, type MinuteRow, mergeCandles } from "./coins";

const row = (bucket: string, source: string, close: string): MinuteRow => ({
  bucket, source, open: "100", high: "110", low: "90", close, volume: "2", trades: 4,
  vwap: "100", latency_p50_ms: 20, latency_p95_ms: 40,
});
const live = (minute: string, source: string, close: string): LiveCandle => ({
  minute_ms: Date.parse(minute), source, open: "100", high: "105", low: "95", close, volume: "1", trades: 2,
});

describe("mergeCandles", () => {
  it("prefers stored minutes and adds the live ones after them", () => {
    const merged = mergeCandles(
      [row("2026-10-10T19:00:00Z", "coinbase", "101"), row("2026-10-10T19:01:00Z", "coinbase", "102")],
      [live("2026-10-10T19:01:00Z", "coinbase", "999"), live("2026-10-10T19:02:00Z", "coinbase", "103")],
      "coinbase",
    );
    expect(merged.map((c) => [c.close, c.live])).toEqual([[101, false], [102, false], [103, true]]);
    expect(merged[0].time).toBe(Date.parse("2026-10-10T19:00:00Z") / 1000);
  });

  it("keeps exchanges apart", () => {
    const merged = mergeCandles([row("2026-10-10T19:00:00Z", "kraken", "50")], [], "coinbase");
    expect(merged).toEqual([]);
  });
});
