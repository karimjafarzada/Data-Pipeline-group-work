-- TEST-ONLY stand-in for Student 3's ops.pipeline_runs migration, copied from CLAUDE.md.
-- The `db` fixture uses it only while no migration file with "pipeline_runs" in its
-- name exists. Delete this file once Student 3's migration is on main.

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
