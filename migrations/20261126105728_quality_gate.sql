-- Quality gate: rows validated by a run reach silver only if that run passed the gate,
-- or a person released them after investigating (CLAUDE.md, "Quality gate").
-- Owner: Student 1.

-- 1. Which run checked each bronze row. This is not bronze.run_id (the run that fetched
--    the row): validate() also checks pending rows left by earlier, crashed runs.
ALTER TABLE bronze.raw_records
    ADD COLUMN validated_run_id UUID REFERENCES ops.pipeline_runs (run_id);

-- Rows checked before this column existed: the best available guess is the run that
-- fetched them. Their runs have no gate result, so they stay held until released.
UPDATE bronze.raw_records
SET validated_run_id = run_id
WHERE validation_status <> 'pending';

-- A pending row has not been checked by any run; a checked row always has its run.
ALTER TABLE bronze.raw_records
    ADD CONSTRAINT raw_records_validated_run_check
    CHECK ((validation_status = 'pending') = (validated_run_id IS NULL));

-- release_run() and the view below look rows up by the run that checked them.
CREATE INDEX raw_records_validated_run_idx
    ON bronze.raw_records (validated_run_id);

-- 2. One row per run: the gate's decision, and what a person did about a held run.
CREATE TABLE ops.quality_gate_results (
    run_id          UUID PRIMARY KEY REFERENCES ops.pipeline_runs (run_id),
    -- The decision, written by run_quality_gate(). All NULL only when the run never
    -- reached the gate (it crashed after validate()) and the row was made by release_run().
    checked         INTEGER CHECK (checked >= 0),  -- records checked, incl. missing_source_id
    failed          INTEGER CHECK (failed BETWEEN 0 AND checked),
    failure_rate    NUMERIC GENERATED ALWAYS AS
                        (CASE WHEN checked > 0 THEN failed::NUMERIC / checked ELSE 0 END) STORED,
    threshold       NUMERIC CHECK (threshold BETWEEN 0 AND 1),
    passed          BOOLEAN,
    decided_at      TIMESTAMPTZ,
    -- What a person did after investigating a held run, written by release_run().
    release_action  TEXT CHECK (release_action IN ('release', 'recheck')),
    released_by     TEXT,
    release_note    TEXT,
    released_at     TIMESTAMPTZ,
    CHECK (num_nulls(checked, failed, threshold, passed, decided_at) IN (0, 5)),
    CHECK (num_nulls(release_action, released_by, release_note, released_at) IN (0, 4)),
    CHECK (passed IS NOT NULL OR release_action IS NOT NULL)
);

-- 3. The only bronze rows build_silver() may promote: valid, and checked by a run that
--    passed the gate or was released. Rows of a run that failed the gate, or never reached
--    it, are held here until release_run() releases or rechecks them.
CREATE VIEW bronze.promotable_records AS
SELECT r.id, r.endpoint, r.source_id, r.payload, r.payload_hash, r.source_updated_at,
       r.fetched_at, r.run_id, r.validation_status, r.validated_run_id
FROM bronze.raw_records AS r
JOIN ops.quality_gate_results AS g ON g.run_id = r.validated_run_id
WHERE r.validation_status = 'valid'
  AND (g.passed OR g.release_action = 'release');
