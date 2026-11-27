-- Differences between the API's columns and the expected schema, found by
-- check_api_schema() and while paging (CLAUDE.md, "API change detection").
-- Owner: Student 1. Needs ops.pipeline_runs (Student 3) from an earlier migration.

CREATE TABLE ops.api_schema_changes (
    id             BIGSERIAL PRIMARY KEY,
    run_id         UUID        NOT NULL REFERENCES ops.pipeline_runs (run_id),
    endpoint       TEXT        NOT NULL,
    change_type    TEXT        NOT NULL
                   CHECK (change_type IN ('column_added', 'column_removed', 'type_changed')),
    column_name    TEXT        NOT NULL,
    expected_type  TEXT,
    actual_type    TEXT,
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, endpoint, change_type, column_name)
);
