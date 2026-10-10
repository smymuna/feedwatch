"""Build dashboard/public/replay/usdc-2023.json: one-minute candles of Kraken USDC/USD from
10 to 13 March 2023, and every incident change the detector made while replaying them.
The dashboard's replay page draws this. Run: python scripts/build_replay.py"""

from __future__ import annotations

import json
from pathlib import Path

from feedwatch.detect.detector import Detector
from feedwatch.replay import history, replay

ROOT = Path(__file__).parents[1]
SOURCE = ROOT / "samples/history/kraken_USDCUSD_2023-03-10_2023-03-13.csv.gz"
OUT = ROOT / "dashboard/public/replay/usdc-2023.json"


def main() -> None:
    candles: dict[int, list[float]] = {}  # minute -> [open, high, low, close, volume, trades]
    detector = Detector(["kraken"], feed_rules=False)
    changes = []
    last_severity: dict[str, str] = {}
    trades = 0
    for event in replay.with_ticks(history.load(SOURCE, "USDC-USD")):
        if event.kind.value == "trade":
            trades += 1
            p, v = float(event.price), float(event.size)  # type: ignore[union-attr]
            minute = event.event_ms // 60_000 * 60  # type: ignore[union-attr]
            c = candles.get(minute)
            if c is None:
                candles[minute] = [p, p, p, p, v, 1]
            else:
                c[1], c[2], c[3] = max(c[1], p), min(c[2], p), p
                c[4] += v
                c[5] += 1
        for inc in detector.process(event):
            # keep the lifecycle only: opened, severity changed, resolved
            first = inc.id not in last_severity
            if first or last_severity[inc.id] != inc.severity.value or not inc.is_open:
                changes.append({
                    "id": inc.id, "type": inc.type.value, "severity": inc.severity.value,
                    "state": "opened" if first else ("resolved" if not inc.is_open else "changed"),
                    "time": inc.updated_ms // 1000,
                    "text": inc.summary if inc.is_open else inc.details.get("resolution", ""),
                    "low": inc.details.get("low"),
                })
            last_severity[inc.id] = inc.severity.value
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({
        "symbol": "USDC-USD", "source": "kraken", "trades": trades,
        "candles": [[t, *(round(x, 6) for x in c[:5]), c[5]] for t, c in sorted(candles.items())],
        "changes": changes,
    }, separators=(",", ":")))
    print(f"{trades:,} trades -> {len(candles):,} candles, {len(changes)} incident changes -> {OUT}")


if __name__ == "__main__":
    main()
