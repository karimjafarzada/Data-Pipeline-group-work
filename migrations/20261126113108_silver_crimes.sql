-- Silver: one typed row per crime record, built by build_silver() from
-- bronze.promotable_records (CLAUDE.md, "Tables" and "Loading rules"). Owner: Student 1.
-- Gold (Student 2) reads only from silver.crimes.

CREATE SCHEMA IF NOT EXISTS silver;

-- Columns in the same order as CrimeRecord (pipeline/quality/model.py), then bronze_id, run_id.
CREATE TABLE silver.crimes (
    id                    BIGINT PRIMARY KEY,           -- API id
    case_number           TEXT        NOT NULL,
    occurred_at           TIMESTAMPTZ NOT NULL,         -- API `date`, as Chicago local time
    block                 TEXT,
    iucr                  TEXT        NOT NULL,
    primary_type          TEXT        NOT NULL,
    description           TEXT,
    location_description  TEXT,
    arrest                BOOLEAN     NOT NULL,
    domestic              BOOLEAN     NOT NULL,
    beat                  TEXT,                         -- text: keeps leading zeros
    district              TEXT,                         -- text: keeps leading zeros ('011')
    ward                  INTEGER,
    community_area        INTEGER,
    fbi_code              TEXT,
    x_coordinate          DOUBLE PRECISION,
    y_coordinate          DOUBLE PRECISION,
    year                  INTEGER     NOT NULL,
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    source_updated_at     TIMESTAMPTZ NOT NULL,         -- API `updated_on`
    bronze_id             BIGINT      NOT NULL REFERENCES bronze.raw_records (id),
    run_id                UUID        NOT NULL REFERENCES ops.pipeline_runs (run_id)
);

-- Gold finds the rows a run inserted or updated by run_id.
CREATE INDEX crimes_run_id_idx ON silver.crimes (run_id);

-- The build_silver() run that handled each bronze row, whether its version won or lost
-- to a newer one. NULL = not handled yet. Only valid rows are ever handled.
ALTER TABLE bronze.raw_records
    ADD COLUMN silver_run_id UUID REFERENCES ops.pipeline_runs (run_id);

ALTER TABLE bronze.raw_records
    ADD CONSTRAINT raw_records_silver_run_check
    CHECK (silver_run_id IS NULL OR validation_status = 'valid');

-- build_silver() reads only valid rows not handled yet. Like the pending index, this one
-- holds only those rows, so incremental runs stay fast however large bronze grows.
-- validated_run_id is the key the promotable view joins on.
CREATE INDEX raw_records_unpromoted_idx
    ON bronze.raw_records (validated_run_id)
    WHERE validation_status = 'valid' AND silver_run_id IS NULL;

-- Same view as before, with silver_run_id added at the end (CREATE OR REPLACE VIEW
-- may only add columns at the end).
CREATE OR REPLACE VIEW bronze.promotable_records AS
SELECT r.id, r.endpoint, r.source_id, r.payload, r.payload_hash, r.source_updated_at,
       r.fetched_at, r.run_id, r.validation_status, r.validated_run_id, r.silver_run_id
FROM bronze.raw_records AS r
JOIN ops.quality_gate_results AS g ON g.run_id = r.validated_run_id
WHERE r.validation_status = 'valid'
  AND (g.passed OR g.release_action = 'release');
