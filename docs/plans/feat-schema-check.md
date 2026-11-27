# Plan: feat/schema-check (API change detection)

Written 2026-11-26 while the team lead was away. The plan was not approved before implementation; review it together with the code.

## Goal

Detect changes to the Chicago Crimes API's columns before they can break silver, as specified in CLAUDE.md ("API change detection"):

- `check_api_schema(run_id)` runs at the start of `ingest()`, before any data is fetched. It reads `<API_BASE_URL>/api/views/ijzp-q8t2.json` and compares each column's `fieldName` and `dataTypeName` with the expected schema, ignoring `:@computed_region_*` columns.
- Every difference is written to `ops.api_schema_changes`.
- A **new column** is logged and the run continues.
- A **removed or retyped column that silver uses** raises `ApiSchemaChangeError` before anything is fetched.
- While paging, a record key that isn't in the expected schema is recorded as `column_added`, once per run. Missing keys are ignored, because the API leaves out nulls.

**Names.** The task asked for "check_schema per page, ops.schema_changes". CLAUDE.md's shared contract names them `check_api_schema` and `ops.api_schema_changes`. This plan follows CLAUDE.md, because renaming would change a shared contract. The question is in `docs/questions.md`.

## Facts checked on real data (2026-11-26)

- **Metadata:** 31 columns. 22 are real; the other 9 are `:@computed_region_*`.
- **Bronze:** across all 1,231,534 payloads, the keys are exactly those 22 real columns, so the per-page check raises no false alarms today.
- **Types:** metadata lists `community_area` as `text`, although its values are integers (silver parses them to `INTEGER`). The expected schema records the API's types as they are.

## Design

1. **`pipeline/ingestion/expected_schema.py`:** `EXPECTED_SCHEMA: dict[str, str]`, the 22 `fieldName -> dataTypeName` pairs as fetched on 2026-11-26.
2. **`pipeline/ingestion/client.py`:** add `SocrataClient.fetch_columns() -> list[dict]`.
   - It requests the metadata URL with the same retries, timeout and `X-App-Token` header as page requests.
   - It raises `ApiResponseError` if the response has no `columns` list.
   - `_get` takes the URL as an argument, so both endpoints share one request path.
3. **`pipeline/ingestion/schema_check.py`:**
   - `SchemaChange(change_type, column_name, expected_type, actual_type)`.
   - `ApiSchemaChangeError`.
   - `compare_schema(expected, actual) -> list[SchemaChange]`: pure, sorted, and ignores computed columns on both sides.
   - `SILVER_SOURCE_COLUMNS`: the API columns silver reads, taken from `CrimeRecord` (each field's alias, e.g. `date`, or its name). A test checks they are all in `EXPECTED_SCHEMA`.
   - `check_api_schema(run_id, *, conn=None, client=None) -> list[SchemaChange]`: fetches and compares the columns, then writes every change in its own committed transaction (`ON CONFLICT DO NOTHING`). After that it raises `ApiSchemaChangeError` if any change is a `column_removed` or `type_changed` for a column in `SILVER_SOURCE_COLUMNS`. Changes are logged as warnings.
   - `record_unexpected_keys(conn, run_id, page, seen) -> int`: the per-page check. It writes `column_added` for keys that aren't in `EXPECTED_SCHEMA`, aren't computed columns, and haven't been seen yet in this run. `actual_type` holds the key's JSON type (`string`, `number`, `boolean`, `object`, `array`), with `json:` in front.
4. **`ingest()`:** after the lock and after finding the start cursor, so that "run a backfill first" still fails without any network call, call `check_api_schema(run_id, conn=conn, client=client)` before the first page is fetched. Inside each page's transaction, call `record_unexpected_keys`, so a key is recorded in the same commit as the page that carried it.
5. **Migration** `<UTC ts>_ops_api_schema_changes.sql`: the table exactly as in CLAUDE.md.

## Tests

- **`compare_schema`:** identical schemas; a column added, removed or retyped; computed columns ignored; deterministic order.
- **`EXPECTED_SCHEMA`** contains every column silver reads.
- **`fetch_columns`:** parses the columns; retries a 503; rejects a response without `columns`; sends the token header.
- **`check_api_schema` with a database:**
  - no change: nothing written;
  - new column: written, run continues;
  - removed or retyped silver column: written, and raises with the column in the message;
  - removed non-silver column (`location`): written, no error;
  - a new computed column: ignored.
- **`ingest()`:**
  - an unknown key on two pages gives one `column_added` row;
  - missing keys write nothing;
  - a removed silver column raises before any data request (the data URL isn't mocked), leaving bronze empty and no checkpoint.
- **Existing ingest and malformed-batch tests** now mock the metadata endpoint too. No test calls the real API.
- **Migration test:** the table and its UNIQUE constraint.

## CLAUDE.md

- Point the interface table at `pipeline/ingestion/schema_check.py`.
- Say which columns count as "used by silver" (the `CrimeRecord` source columns).
- Describe the `json:` prefix in `actual_type` for keys found in records.
- Add an open item for Student 3: the CLI should mark the run `failed` on `ApiSchemaChangeError`.

## Commits

1. `docs: plan API change detection`
2. `feat(ingestion): add ops.api_schema_changes`
3. `feat(ingestion): fetch the dataset's column metadata`
4. `feat(ingestion): detect API schema changes before and during ingestion`
5. `docs: document API change detection`
