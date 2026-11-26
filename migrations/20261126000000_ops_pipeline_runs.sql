-- Run history must exist before ingestion migrations reference run_id.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE ops.pipeline_runs (
    run_id            UUID PRIMARY KEY,
    mode              TEXT NOT NULL CHECK (mode IN ('incremental', 'backfill')),
    backfill_start    DATE,
    backfill_end      DATE,
    status            TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    rows_fetched      INTEGER DEFAULT 0,
    rows_valid        INTEGER DEFAULT 0,
    rows_quarantined  INTEGER DEFAULT 0,
    error_message     TEXT
);
