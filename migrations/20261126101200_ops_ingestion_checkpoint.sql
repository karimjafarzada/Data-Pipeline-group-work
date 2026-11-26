-- Where each ingestion run stopped, so the next one can continue (CLAUDE.md, "Tables").
-- Owner: Student 1. Needs ops.pipeline_runs (Student 3) from an earlier migration.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE ops.ingestion_checkpoint (
    checkpoint_key  TEXT PRIMARY KEY,  -- 'incremental:<endpoint>' or 'backfill:<endpoint>:<start>:<end>'
    cursor_value    TEXT NOT NULL,     -- incremental: '<updated_on>|<id>'; backfill: '<id>'
    last_run_id     UUID REFERENCES ops.pipeline_runs (run_id),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
