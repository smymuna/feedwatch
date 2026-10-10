-- FeedWatch storage (PostgreSQL + TimescaleDB). Idempotent: safe to run on every start.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Every incident, latest version only (upserted by id as it opens, changes and resolves).
CREATE TABLE IF NOT EXISTS incidents (
    id           text PRIMARY KEY,
    type         text        NOT NULL,
    severity     text        NOT NULL,
    status       text        NOT NULL CHECK (status IN ('open', 'resolved')),
    source       text,
    symbol       text,
    opened_at    timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    resolved_at  timestamptz,
    summary      text        NOT NULL,
    details      jsonb       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS incidents_open_idx ON incidents (status, opened_at DESC);
CREATE INDEX IF NOT EXISTS incidents_recent_idx ON incidents (updated_at DESC);

-- One row per source, symbol and minute (event time), from the windowed metrics job.
CREATE TABLE IF NOT EXISTS metrics_1m (
    bucket          timestamptz NOT NULL,
    source          text        NOT NULL,
    symbol          text        NOT NULL,
    trades          integer     NOT NULL,
    volume          numeric     NOT NULL,
    vwap            numeric     NOT NULL,
    open            numeric     NOT NULL,
    high            numeric     NOT NULL,
    low             numeric     NOT NULL,
    close           numeric     NOT NULL,
    latency_p50_ms  integer,
    latency_p95_ms  integer,
    PRIMARY KEY (symbol, source, bucket)
);
SELECT create_hypertable('metrics_1m', 'bucket', if_not_exists => TRUE);

-- Live state, overwritten about once a second by the detector's health snapshots.
CREATE TABLE IF NOT EXISTS feed_health (
    source               text PRIMARY KEY,
    connected            boolean     NOT NULL,
    last_message_at      timestamptz,
    last_trade_at        timestamptz,
    messages_per_s       real        NOT NULL DEFAULT 0,
    trades_per_s         real        NOT NULL DEFAULT 0,
    latency_p95_ms       integer,
    gaps_total           bigint      NOT NULL DEFAULT 0,
    missing_trades_total bigint      NOT NULL DEFAULT 0,
    updated_at           timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS symbol_health (
    symbol         text NOT NULL,
    source         text NOT NULL,
    last_price     numeric,
    vwap_60s       numeric,
    trades_60s     integer     NOT NULL DEFAULT 0,
    peg_dev_pct    double precision,          -- stablecoins only: (vwap - 1) * 100
    divergence_bps double precision,          -- vs the other exchange, when both are fresh
    last_trade_at  timestamptz,
    updated_at     timestamptz NOT NULL,
    PRIMARY KEY (symbol, source)
);
