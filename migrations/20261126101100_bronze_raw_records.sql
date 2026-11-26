-- Bronze: every API record exactly as received (CLAUDE.md, "Tables"). Owner: Student 1.
-- Needs ops.pipeline_runs (Student 3), which must come from an earlier migration.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE bronze.raw_records (
    id                 BIGSERIAL PRIMARY KEY,
    endpoint           TEXT        NOT NULL,  -- dataset ID, e.g. 'ijzp-q8t2'
    source_id          TEXT        NOT NULL,  -- the record's own ID in the API
    payload            JSONB       NOT NULL,  -- the API item exactly as received
    payload_hash       TEXT        NOT NULL,  -- sha256 of the payload with sorted keys
    source_updated_at  TIMESTAMPTZ,           -- API's updated_on, as Chicago local time
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id             UUID        NOT NULL REFERENCES ops.pipeline_runs (run_id),
    validation_status  TEXT        NOT NULL DEFAULT 'pending'
                       CHECK (validation_status IN ('pending', 'valid', 'invalid')),
    UNIQUE (endpoint, source_id, payload_hash)
);

-- Finds where the first incremental run starts (newest source_updated_at per endpoint).
CREATE INDEX raw_records_endpoint_updated_idx
    ON bronze.raw_records (endpoint, source_updated_at);
