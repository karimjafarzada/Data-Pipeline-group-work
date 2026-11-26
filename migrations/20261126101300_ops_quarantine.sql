-- Records that failed a rule, kept with the reason (CLAUDE.md, "Tables"). Owner: Student 1.
-- Needs ops.pipeline_runs (Student 3) from an earlier migration.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE ops.quarantine (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID        NOT NULL REFERENCES ops.pipeline_runs (run_id),
    endpoint        TEXT        NOT NULL,
    source_id       TEXT,                 -- NULL only for rule 'missing_source_id'
    payload         JSONB       NOT NULL,
    payload_hash    TEXT        NOT NULL,
    rule_name       TEXT        NOT NULL, -- e.g. 'missing_required_field'
    reason          TEXT        NOT NULL, -- human-readable detail
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (payload_hash, rule_name)
);
