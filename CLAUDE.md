# CLAUDE.md

## Project
Python data pipeline: one public API (Chicago Data Portal, "Crimes - 2001 to Present",
Socrata SODA endpoint https://data.cityofchicago.org/resource/ijzp-q8t2.json) → PostgreSQL in bronze / silver / gold layers,
with analytical SQL on gold. Everything runs in Docker. Team of 3; see README for members.

## My scope (Student 1)
API client, resumable ingestion, API change detection, validation rules, quarantine,
quality gate, silver layer. Do not change gold tables (Student 2) or Docker / CI /
orchestration / run tracking (Student 3) without asking first.

## Working rules
- One feature branch per task (`feat/...`, `fix/...`, `test/...`). Never commit to or push to `main`.
- Conventional commits (`feat:`, `fix:`, `test:`, `docs:`, `refactor:`, `chore:`), small and atomic.
- Every module gets pytest tests. Mock the API in tests; never call the real API from tests.
- No secrets in code or in the repo. Read all config from environment variables (see below).
- Always show a plan before writing code, and run `pytest` before committing.
- Explain any non-obvious code when asked; I must be able to explain every line.

## API notes
- Base URL is `API_BASE_URL` (`https://data.cityofchicago.org`); the dataset ID `ijzp-q8t2` is a
  constant in code. The resource URL is `<API_BASE_URL>/resource/ijzp-q8t2.json`, and bronze's
  `endpoint` column stores `ijzp-q8t2`.
- Responses are a plain JSON array with no total count: stop when a page returns fewer rows than `$limit`.
- All values arrive as JSON strings except booleans (`arrest`, `domestic`) and the nested `location`
  object. `id` is numeric in SoQL, so compare it unquoted: `id > 123`.
- **Null fields are omitted.** A record with no coordinates has no `latitude` key at all.
  A missing key is normal and is never by itself an API change.
- **Timestamps have no timezone** (`2024-06-09T18:30:00.000`). They are Chicago local time:
  attach `America/Chicago` when converting to `TIMESTAMPTZ`. In SoQL filters and in checkpoint
  cursors, use the raw string exactly as the API returned it.
- **Paging is keyset, never `$offset`.** Offsets shift when records are updated mid-run, which
  silently skips rows. Many records share one `updated_on`, so `id` is always the tiebreaker:
  - Incremental: `$order=updated_on,id` and
    `$where=updated_on > '<ts>' OR (updated_on = '<ts>' AND id > <last_id>)`.
    Cursor = `<updated_on>|<id>` of the last row of the page. With no checkpoint yet, start from
    the latest `source_updated_at` in bronze; if bronze is empty, fail and ask for a backfill.
  - Backfill: `$order=id` and `$where=date >= '<start>T00:00:00' AND date < '<end + 1 day>T00:00:00' AND id > <last_id>`.
    Cursor = last `id`. The end date is inclusive; never use `between`, which drops most of the last day.
    Re-running a finished backfill fetches only records added to that range since.
- `source_id` in bronze = `id`. `source_updated_at` = `updated_on`.
- Records can be updated after first publication (see `updated_on`); those changes feed the SCD2
  dimension in gold. Because `updated_on` is part of the payload, every update produces a new
  hash, so a record that changes A → B → A still gets a new bronze row. Check which fields
  actually change once data is loaded.
- Errors: on HTTP 429 and 5xx (a 503 was seen in testing), back off exponentially and retry.
  No quota headers are returned. After the retries run out, fail the run; the checkpoint is
  still at the last committed page.
- Metadata: `<API_BASE_URL>/api/views/ijzp-q8t2.json` lists the dataset's columns
  (`fieldName`, `dataTypeName`). It also lists `:@computed_region_*` columns that record
  responses do not include; ignore those.

## Shared contracts (agreed with the team, change only via a reviewed PR)

### Schemas
`bronze`, `silver`, `gold`, and `ops` (pipeline bookkeeping).
Migration order: `ops` first, because bronze references `ops.pipeline_runs` (see "Migrations").

### Migrations
- SQL files live in `migrations/`, named `<UTC timestamp>_<description>.sql`: 14 digits, then
  lowercase letters, digits and underscores, e.g. `20261126101100_bronze_raw_records.sql`.
  Take the timestamp from `date -u +%Y%m%d%H%M%S` when you create the file, so teammates
  never pick the same name. Any other name makes the runner stop with an error.
- Files are applied in name order, so a table must be created by an older file than any file
  that references it. `ops.pipeline_runs` (Student 3) must sort before `20261126101100`,
  e.g. `20261126000000_ops_pipeline_runs.sql`, because bronze, checkpoint and quarantine reference it.
- Apply them with `python -m pipeline.migrate` (reads the `POSTGRES_*` variables). Each file
  runs once, in its own transaction, together with its row in
  `ops.schema_migrations (filename, applied_at)`, so a failed file leaves no trace and running
  the command again applies only new files. An advisory lock stops two runners at once.
- Never edit a migration that has been applied anywhere; add a new file instead.
  Don't put `BEGIN` / `COMMIT` in migration files.
- Tests apply every migration to a fresh throwaway database. Until Student 3's `pipeline_runs`
  migration exists, they first load the test-only copy in `tests/sql/ops_pipeline_runs_stub.sql`.

### run_id
- A UUID created once at the start of every run by `start_run()` (owner: Student 3).
- It is inserted into `ops.pipeline_runs` with status `running` before any data is fetched.
- It is passed explicitly to every step, which run in this order:
  `ingest(run_id, ...)` → `validate(run_id)` → `run_quality_gate(run_id, ...)` → `build_silver(run_id)` → `load_gold(run_id)`.
  If a step raises, the steps after it don't run.
- Every row written to bronze, quarantine and silver stores the `run_id` that wrote it.
- At the end, `finish_run()` sets status `success` or `failed` with counts and the error message.

### Tables

```sql
-- Owner: Student 3
CREATE TABLE ops.pipeline_runs (
    run_id            UUID PRIMARY KEY,
    mode              TEXT NOT NULL CHECK (mode IN ('incremental', 'backfill')),
    backfill_start    DATE,
    backfill_end      DATE,
    status            TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    rows_fetched      INTEGER DEFAULT 0,  -- records the API returned, including ones bronze already had
    rows_valid        INTEGER DEFAULT 0,  -- records that passed validation in this run
    rows_quarantined  INTEGER DEFAULT 0,  -- distinct records quarantined in this run (not rule hits)
    error_message     TEXT
);

-- Owner: Student 1
CREATE TABLE bronze.raw_records (
    id                 BIGSERIAL PRIMARY KEY,
    endpoint           TEXT        NOT NULL,  -- dataset ID, e.g. 'ijzp-q8t2'
    source_id          TEXT        NOT NULL,  -- the record's own ID in the API
    payload            JSONB       NOT NULL,  -- the API item exactly as received
    payload_hash       TEXT        NOT NULL,  -- see "Payload hash" below
    source_updated_at  TIMESTAMPTZ,           -- API's updated_on, as Chicago local time
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id             UUID        NOT NULL REFERENCES ops.pipeline_runs (run_id),
    validation_status  TEXT        NOT NULL DEFAULT 'pending'
                       CHECK (validation_status IN ('pending', 'valid', 'invalid')),
    validated_run_id   UUID REFERENCES ops.pipeline_runs (run_id),  -- the run whose validate() checked it
    silver_run_id      UUID REFERENCES ops.pipeline_runs (run_id),  -- the build_silver() run that handled it
    UNIQUE (endpoint, source_id, payload_hash),
    CHECK ((validation_status = 'pending') = (validated_run_id IS NULL)),
    CHECK (silver_run_id IS NULL OR validation_status = 'valid')
);
-- Partial index raw_records_unpromoted_idx: valid rows with silver_run_id IS NULL.

-- Owner: Student 1
CREATE TABLE ops.ingestion_checkpoint (
    checkpoint_key  TEXT PRIMARY KEY,  -- 'incremental:<endpoint>' or 'backfill:<endpoint>:<start>:<end>'
    cursor_value    TEXT NOT NULL,     -- incremental: '<updated_on>|<id>'; backfill: '<id>'
    last_run_id     UUID REFERENCES ops.pipeline_runs (run_id),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Owner: Student 1
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

-- Owner: Student 1. One row per run: the gate's decision, and a person's release of a held run.
CREATE TABLE ops.quality_gate_results (
    run_id          UUID PRIMARY KEY REFERENCES ops.pipeline_runs (run_id),
    checked         INTEGER,      -- decision (run_quality_gate); all five NULL only when the run
    failed          INTEGER,      -- never reached the gate and release_run() made the row
    failure_rate    NUMERIC GENERATED ALWAYS AS (failed / checked, 0 when checked = 0) STORED,
    threshold       NUMERIC,
    passed          BOOLEAN,
    decided_at      TIMESTAMPTZ,
    release_action  TEXT CHECK (release_action IN ('release', 'recheck')),  -- release (release_run)
    released_by     TEXT,
    release_note    TEXT,
    released_at     TIMESTAMPTZ
);
-- Exact constraints: migrations/20261126105728_quality_gate.sql.

-- Owner: Student 1. The only bronze rows build_silver() may promote.
CREATE VIEW bronze.promotable_records AS  -- same columns as bronze.raw_records
SELECT r.* FROM bronze.raw_records r
JOIN ops.quality_gate_results g ON g.run_id = r.validated_run_id
WHERE r.validation_status = 'valid' AND (g.passed OR g.release_action = 'release');

-- Owner: Student 1
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

-- Owner: Student 1
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
```

The nested `location` object is not copied to silver; it duplicates `latitude` / `longitude`.
Gold (owner: Student 2) reads only from silver. Rows a run inserted or updated in silver carry that run's `run_id`.

### Payload hash
`sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))`
as a hex string. The same function is used for bronze and quarantine.

### Loading rules
- Bronze insert: `ON CONFLICT (endpoint, source_id, payload_hash) DO NOTHING`.
  Re-fetching the same record adds nothing; a changed record (new hash) adds a new row,
  which keeps the change history that the SCD2 dimension in gold needs.
- A record with no `id` cannot go to bronze. Ingestion writes it straight to quarantine with
  rule `missing_source_id` and `source_id` NULL, and it counts toward `rows_quarantined`.
- Each page is written to bronze (and any `missing_source_id` rows to quarantine) and its
  checkpoint is updated **in the same transaction**, so a crash can never leave them out of sync.
  On startup, ingestion resumes from the checkpoint.
- Incremental and backfill runs use separate checkpoint keys and never overwrite each other.
- `validate(run_id)` checks **every** bronze row with `validation_status = 'pending'`, not only
  this run's rows, so rows left by a crashed run are picked up. Each row becomes `valid` or `invalid`,
  and `validated_run_id` is set to the run that checked it, in the same update.
- Quarantine insert: one row per broken rule, `ON CONFLICT (payload_hash, rule_name) DO NOTHING`.
- `build_silver(run_id) -> SilverStats` (`pipeline/silver/build.py`): reads **only**
  `bronze.promotable_records`: valid rows whose validating run passed the quality gate or was
  released (see "Quality gate"). Valid rows of a run that failed the gate are **held**: no later run
  publishes them until a person releases them.
  - It reads only rows no earlier build handled (`silver_run_id IS NULL`), through a partial index,
    so incremental builds read only new rows. At the end it sets `silver_run_id` on every row it
    read, winners and older versions alike, all in one transaction.
  - For each `source_id`, it takes the newest of those rows (by `source_updated_at`, then bronze `id`),
    builds a `CrimeRecord`, `COPY`s it into a temp table, and upserts that in one statement with
    `ON CONFLICT (id) DO UPDATE ... WHERE (silver.crimes.source_updated_at, silver.crimes.bronze_id) < (EXCLUDED.source_updated_at, EXCLUDED.bronze_id)`.
    Strictly newer only: an older version never overwrites a newer one, and a re-run never rewrites
    a row with itself. Every inserted or updated row gets this `run_id`.
  - A promotable row that no longer fits `CrimeRecord` is not skipped silently: it is quarantined as
    `model_mismatch`, its bronze row becomes `invalid`, and it counts in `SilverStats.rejected`.
    The record's next newest candidate takes its place.
  - If a record's newest version is invalid or held, silver keeps its last promotable version.
  - `SilverStats`: `candidates` (rows read), `records` (distinct `source_id`s), `inserted`, `updated`,
    `unchanged` (silver already had this version or a newer one), `rejected`, `seconds`.
  - An advisory lock stops two builds at once (`SilverBuildError`).

### Validation rules (extend after profiling the data)
Code: `pipeline/quality/rules.py`. Each rule gives at most one quarantine row per record;
several problems for the same rule are joined into one `reason`. A missing key fails only
`missing_required_field`; the other rules skip missing values, and rules that compare two fields
skip values that don't parse, so one bad value is reported once. A blank string counts as missing.

| Rule | Fails when |
|---|---|
| `missing_source_id` | no `id` (checked at ingestion) |
| `missing_required_field` | any of `case_number`, `date`, `iucr`, `primary_type`, `arrest`, `domestic`, `year`, `updated_on` is missing |
| `invalid_timestamp` | `date` or `updated_on` does not parse, or `date` is in the future |
| `invalid_number` | `id`, `ward`, `community_area` or `year` is present but not an integer, or a coordinate (`x_coordinate`, `y_coordinate`, `latitude`, `longitude`) is present but not a finite number |
| `invalid_boolean` | `arrest` or `domestic` is present but not a JSON `true` / `false` |
| `year_mismatch` | `year` differs from the year of `date` |
| `coordinates_incomplete` | only one of `latitude` / `longitude` is present |
| `coordinates_out_of_range` | latitude outside 41.6–42.1 or longitude outside −88.0 to −87.5 (when present) |
| `updated_before_date` | `updated_on` is earlier than `date` |
| `model_mismatch` | the record passes every rule above but still doesn't fit `CrimeRecord` (`pipeline/quality/model.py`), which means a rule is missing. `build_silver()` checks again and uses the same rule if the model changed after validation |

`CrimeRecord` (Pydantic) is the typed shape of a valid record, field for field the same as `silver.crimes`.

### Quality gate
Code: `pipeline/quality/gate.py` and `pipeline/quality/release.py`.
- A separate step after `validate()`: `run_quality_gate(run_id, ingest_stats, validation_stats, *, conn=None, threshold=None)`.
  `check_quality_gate(ingest_stats, validation_stats, threshold)` is the same check without the database.
- failure rate = `rows_quarantined / (rows_valid + rows_quarantined)` for this run.
  Both count distinct records, so a record breaking two rules counts once.
- The rate is based on the rows actually checked, **not** on `rows_fetched`, because `validate()`
  also checks pending rows left by earlier runs, and `rows_fetched` includes records bronze already had:
  `(ValidationStats.invalid + IngestStats.rows_quarantined) / (ValidationStats.checked + IngestStats.rows_quarantined)`,
  where `IngestStats.rows_quarantined` is the `missing_source_id` records that never reached bronze.
  The same sums go into `ops.pipeline_runs`: `rows_valid = ValidationStats.valid`,
  `rows_quarantined = ValidationStats.invalid + IngestStats.rows_quarantined`.
- If nothing was checked (zero records), the gate passes. Zero records in bronze but some
  `missing_source_id` records gives a rate of 100%, which fails.
- "Exceeds" means **strictly greater**: a rate equal to the threshold passes (1 in 20 at `0.05` passes).
  The rate and threshold are compared as exact fractions, not floats.
- `QUALITY_MAX_FAILURE_RATE` (default `0.05`; unset or blank means the default) must be a number from
  0 to 1, otherwise `ValueError`. 0 fails on any failure; 1 never fails.
- The default assumes **daily runs** (several hundred records each; 2015-06 had 23,203 records and a
  0% failure rate). There is no minimum run size, so in much smaller runs (e.g. hourly) a single
  broken record can exceed 5%; revisit the threshold if runs become more frequent.
- The decision is written to `ops.quality_gate_results` **before** raising. If the rate exceeds the
  threshold, `QualityGateError` is raised (its `result` holds the numbers), the run is marked
  `failed`, and silver and gold are **not** touched for that run.
  Bronze and quarantine keep their rows, so the failure can be investigated.
- A failed run's valid rows stay **held** (not in `bronze.promotable_records`), so a later successful
  run can't publish them. The same goes for a run that crashed after `validate()` and never reached
  the gate. After investigating, a person calls
  `release_run(run_id, action, released_by=..., note=...)`, once per run:
  - `release`: the rows are fine; the next successful run's `build_silver()` promotes them.
  - `recheck`: a rule was missing; after adding it, the run's valid rows go back to `pending`, so
    the next run validates them under the new rules and its gate counts them.

  It raises `ReleaseError` for a run that passed, was already released or rechecked, or has no held rows.

### API change detection
- `check_api_schema(run_id)` runs at the start of `ingest()`, before any data is fetched. It reads the
  metadata endpoint and compares `fieldName` / `dataTypeName` with the expected schema in
  `pipeline/ingestion/expected_schema.py`, ignoring `:@computed_region_*` columns.
- Every difference is written to `ops.api_schema_changes`.
- A **new column** is logged and the run continues; bronze keeps it in `payload`.
- A **removed or retyped column** that silver uses raises `ApiSchemaChangeError`, and the run fails
  before anything is fetched. "Used by silver" means the `CrimeRecord` source columns (each field's
  alias, e.g. `date`, or its name): every expected column except `location`. A removed or retyped
  column silver doesn't use is recorded and the run continues.
- The changes are committed **before** `ApiSchemaChangeError` is raised, so a failed run shows why.
- While paging, a record key not in the expected schema is recorded as `column_added`
  (once per run), in the same transaction as its page, with `actual_type` = its JSON type
  prefixed with `json:` (e.g. `json:string`). Missing keys are ignored, because the API omits nulls.
- Code: `pipeline/ingestion/schema_check.py` (`check_api_schema`, `compare_schema`,
  `record_unexpected_keys`) and `pipeline/ingestion/expected_schema.py` (copied from the live
  metadata on 2026-11-26: 22 columns). `SocrataClient.fetch_columns()` reads the metadata with the
  same retries as pages.

### Python interfaces
| Function | Module | Owner |
|---|---|---|
| `get_connection()` (retries until the DB is ready) | `pipeline/db.py` | Student 3 |
| `start_run(mode, start=None, end=None) -> UUID` | `pipeline/runs.py` | Student 3 |
| `finish_run(run_id, status, counts, error=None)` | `pipeline/runs.py` | Student 3 |
| `ingest(run_id, mode, start=None, end=None) -> IngestStats` | `pipeline/ingestion/` | Student 1 |
| `check_api_schema(run_id) -> list[SchemaChange]` (raises `ApiSchemaChangeError`) | `pipeline/ingestion/schema_check.py` | Student 1 |
| `validate(run_id) -> ValidationStats` | `pipeline/quality/` | Student 1 |
| `run_quality_gate(run_id, ingest_stats, validation_stats) -> GateResult` (raises `QualityGateError`) | `pipeline/quality/gate.py` | Student 1 |
| `release_run(run_id, action, *, released_by, note) -> int` (raises `ReleaseError`) | `pipeline/quality/release.py` | Student 1 |
| `build_silver(run_id) -> SilverStats` (raises `SilverBuildError`) | `pipeline/silver/build.py` | Student 1 |
| `load_gold(run_id)` | `pipeline/gold/` | Student 2 |

CLI entrypoint (scheduled by Student 3):
`python -m pipeline run --mode incremental`
`python -m pipeline run --mode backfill --start YYYY-MM-DD --end YYYY-MM-DD`

### Environment variables
`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`,
`API_BASE_URL`, `SOCRATA_APP_TOKEN` (sent as the `X-App-Token` header), `QUALITY_MAX_FAILURE_RATE`,
`API_INCREMENTAL_TIMEOUT_SECONDS` (optional, default `120`: the timeout for incremental "changed since"
requests, which are slow on the API's side; backfill requests use 30 seconds).
`POSTGRES_HOST` / `POSTGRES_PORT` are where the pipeline connects: `db` / `5432` inside Docker,
`127.0.0.1` / the published port from the host.
Real values live in `.env` (gitignored); `.env.example` lists the names with dummy values.

## Open items (owner: Student 3)
- `compose.yaml` on `feat/docker-postgres` uses `POSTGRES_PORT` for the published host port (5433).
  That clashes with the meaning above; rename the published one (e.g. `POSTGRES_HOST_PORT`).
- Add `POSTGRES_HOST`, `API_BASE_URL`, `SOCRATA_APP_TOKEN`, `QUALITY_MAX_FAILURE_RATE` and
  `API_INCREMENTAL_TIMEOUT_SECONDS` to `.env.example`.
- README does not list team members yet.
- The CLI must call `run_quality_gate(run_id, ingest_stats, validation_stats)` between `validate()` and
  `build_silver()` (see "run_id"), and on `QualityGateError` call `finish_run(..., 'failed', ...)` with
  `rows_valid = ValidationStats.valid` and `rows_quarantined = GateResult.failed`.
- Whether to add a CLI command for `release_run()` is still to be agreed.
- `ingest()` raises `ApiSchemaChangeError` when the API removed or retyped a column silver uses;
  the CLI should then call `finish_run(..., 'failed', ...)` with the error message.
