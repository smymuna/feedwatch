import { describe, expect, it } from "vitest";
import { ago, duration, gaugePosition, pct, pegLevel, price } from "./format";

const NOW = Date.parse("2026-10-10T19:00:00Z");

describe("format", () => {
  it("says how long ago", () => {
    expect(ago("2026-10-10T18:59:57Z", NOW)).toBe("3 s ago");
    expect(ago("2026-10-10T18:55:00Z", NOW)).toBe("5 min ago");
    expect(ago(null, NOW)).toBe("never");
  });

  it("formats durations, open or closed", () => {
    expect(duration("2026-10-10T18:58:30Z", null, NOW)).toBe("1 min 30 s");
    expect(duration("2026-03-11T00:35:00Z", "2026-03-13T13:53:00Z", NOW)).toBe("61 h 18 min");
  });

  it("shows stablecoin prices with five decimals and big prices with two", () => {
    expect(price("0.99897")).toBe("0.99897");
    expect(price("62000.1534")).toBe("62,000.15");
    expect(price(null)).toBe("–");
  });

  it("uses a real minus sign", () => {
    expect(pct(-1.5)).toBe("−1.50 %");
    expect(pct(0.25, 1)).toBe("+0.3 %");
  });

  it("clamps the peg gauge", () => {
    expect(gaugePosition(0)).toBe(50);
    expect(gaugePosition(-1)).toBe(25);
    expect(gaugePosition(-12)).toBe(0);
  });

  it("classifies peg deviations", () => {
    expect(pegLevel(-0.1)).toBe("ok");
    expect(pegLevel(-0.6)).toBe("warning");
    expect(pegLevel(-12.1)).toBe("critical");
    expect(pegLevel(null)).toBe("none");
  });
});
