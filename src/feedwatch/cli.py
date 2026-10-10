"""``feedwatch <command>``: one entry point for every service and tool.

topics      create the Kafka topics
ingest      stream both exchanges into Kafka (--record keeps the raw messages)
metrics     one-minute metrics job (Quix Streams)
detect      the detection service
store       write incidents, metrics and health to PostgreSQL
api         the read API and dashboard
history     download historical trades from Kraken (e.g. the March 2023 USDC depeg)
replay      replay a recording or history file into Kafka, optionally with faults
backtest    run the detector over a recording or history file offline and print incidents
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from feedwatch.config import get_settings
from feedwatch.models import Event, Incident, Severity


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(), format="%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _date(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _events(args: argparse.Namespace) -> Iterator[Event]:
    from feedwatch.replay import history, replay, scenarios

    path = Path(args.input)
    if path.is_dir():
        events: Iterator[Event] = replay.read_recording(path)
    else:
        if not args.symbol:
            sys.exit("--symbol is required for a history file, e.g. --symbol USDC-USD")
        events = iter(history.load(path, args.symbol))
    if args.scenario:
        events = scenarios.inject(events, scenarios.load(args.scenario).faults)
    return replay.with_ticks(events)


def cmd_topics(_: argparse.Namespace) -> None:
    from feedwatch.kafka import ensure_topics

    created = ensure_topics(get_settings())
    print("created: " + ", ".join(created) if created else "all topics already exist")


def cmd_ingest(args: argparse.Namespace) -> None:
    from feedwatch.ingest.runner import RawRecorder, run
    from feedwatch.kafka import JsonProducer, ensure_topics
    from feedwatch.sources.base import Source
    from feedwatch.sources.coinbase import Coinbase
    from feedwatch.sources.kraken import Kraken

    s = get_settings()
    ensure_topics(s)
    producer = JsonProducer(s, "feedwatch-ingest")

    def dead_letter(source: str, raw: str, exc: Exception) -> None:
        producer.send(s.topic_dlq, source, {"source": source, "error": repr(exc), "raw": raw})

    recorder = RawRecorder(s.raw_dir) if (args.record or s.record_raw) else None
    sources: list[Source] = [
        Coinbase(products=s.coinbase_products, url=s.coinbase_url),
        Kraken(symbols=s.kraken_symbols, url=s.kraken_url),
    ]
    try:
        asyncio.run(run(sources, producer.send_event, dead_letter, recorder))
    finally:
        producer.flush()


def cmd_metrics(_: argparse.Namespace) -> None:
    from feedwatch.metrics.job import run

    run(get_settings())


def cmd_detect(_: argparse.Namespace) -> None:
    from feedwatch.detect.service import run

    run(get_settings())


def cmd_store(_: argparse.Namespace) -> None:
    from feedwatch.store.writer import run

    run(get_settings())


def cmd_api(args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run("feedwatch.api.app:app", host=args.host, port=args.port, log_level="info")


def cmd_history(args: argparse.Namespace) -> None:
    from feedwatch.replay.history import fetch_kraken_trades, save

    start, end = _date(args.start), _date(args.end)
    out = Path(
        args.out or f"data/history/kraken_{args.pair}_{start:%Y-%m-%d}_{end:%Y-%m-%d}.csv.gz"
    )
    count = save(fetch_kraken_trades(args.pair, start, end), out)
    print(f"saved {count:,} trades to {out}")


def cmd_replay(args: argparse.Namespace) -> None:
    from feedwatch.kafka import JsonProducer, ensure_topics
    from feedwatch.replay.replay import paced, rebase

    s = get_settings()
    ensure_topics(s)
    producer = JsonProducer(s, "feedwatch-replay")
    events = _events(args)
    if not args.keep_times:
        events = rebase(events, time.time_ns() // 1_000_000)
    count = 0
    for event in paced(events, args.speed):
        producer.send_event(event)
        count += 1
    producer.flush()
    print(f"replayed {count:,} events")


def _print_incident(inc: Incident) -> None:
    when = datetime.fromtimestamp(inc.updated_ms / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S")
    state = "OPEN    " if inc.is_open else "RESOLVED"
    mark = {Severity.CRITICAL: "!!", Severity.WARNING: "! ", Severity.INFO: "  "}[inc.severity]
    text = inc.summary if inc.is_open else inc.details.get("resolution", inc.summary)
    print(f"{when}  {mark} {state} {inc.type.value:<16} {text}")


def cmd_backtest(args: argparse.Namespace) -> None:
    from feedwatch.detect.detector import Detector

    s = get_settings()
    recording = Path(args.input).is_dir()
    det = Detector(
        ["coinbase", "kraken"] if recording else ["kraken"], s.thresholds, feed_rules=recording
    )  # trade history has no heartbeats to judge a feed by
    seen: list[Incident] = []
    severity: dict[str, Severity] = {}
    for event in _events(args):
        for inc in det.process(event):
            seen.append(inc)
            # print the lifecycle only: opened, severity changed, resolved
            if not args.quiet and (
                inc.id not in severity or severity[inc.id] != inc.severity or not inc.is_open
            ):
                _print_incident(inc)
            severity[inc.id] = inc.severity
    print(f"\n{len(severity)} incident(s), {len(det.open)} still open at the end")
    if args.json:
        Path(args.json).write_text(json.dumps([i.to_message() for i in seen], indent=2))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="feedwatch", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--log-level", default="info")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("topics").set_defaults(fn=cmd_topics)
    p = sub.add_parser("ingest")
    p.add_argument("--record", action="store_true", help="also keep raw messages in data/raw")
    p.set_defaults(fn=cmd_ingest)
    sub.add_parser("metrics").set_defaults(fn=cmd_metrics)
    sub.add_parser("detect").set_defaults(fn=cmd_detect)
    sub.add_parser("store").set_defaults(fn=cmd_store)
    p = sub.add_parser("api")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8000)
    p.set_defaults(fn=cmd_api)

    p = sub.add_parser("history", help="download Kraken trade history")
    p.add_argument("pair", help="Kraken REST pair, e.g. USDCUSD")
    p.add_argument("start", help="UTC, e.g. 2023-03-10T12:00")
    p.add_argument("end")
    p.add_argument("--out")
    p.set_defaults(fn=cmd_history)

    for name, fn in (("replay", cmd_replay), ("backtest", cmd_backtest)):
        p = sub.add_parser(name)
        p.add_argument("input", help="a recording directory or a history .csv.gz")
        p.add_argument("--symbol", help="canonical symbol for a history file, e.g. USDC-USD")
        p.add_argument("--scenario", help="a fault scenario JSON from scenarios/")
        if name == "replay":
            p.add_argument(
                "--speed",
                type=float,
                default=1.0,
                help="1 = real time, 60 = a minute per second, 0 = as fast as possible",
            )
            p.add_argument(
                "--keep-times",
                action="store_true",
                help="keep original timestamps instead of starting 'now'",
            )
        else:
            p.add_argument("--quiet", action="store_true")
            p.add_argument("--json", help="also write all incident updates to this file")
        p.set_defaults(fn=fn)

    args = parser.parse_args(argv)
    _setup_logging(args.log_level)
    args.fn(args)


if __name__ == "__main__":
    main()
