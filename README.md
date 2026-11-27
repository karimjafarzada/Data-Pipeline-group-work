# Data-Pipeline-group-work

## Data quality

Every record from the Chicago Crimes API is checked before it can reach the silver layer. A run whose share of broken records is too high is stopped, and its records are held back until someone has looked at them.

Each run goes through these steps, in order:

```
ingest → validate → quality gate → build_silver → load_gold
```

- **`ingest`** stores every record as received in `bronze.raw_records`. A record with no `id` can't be stored in bronze, so it goes straight to quarantine (rule `missing_source_id`).
- **`validate`** checks every bronze row still marked `pending`. Each row becomes `valid` or `invalid`, and every rule an invalid row breaks is written to `ops.quarantine` with a readable reason.
- **The quality gate** decides whether the run is good enough to publish (see below).
- **`build_silver`** promotes only rows that are valid **and** belong to a run that passed the gate, or that someone released.

### Validation rules

Code: [`src/pipeline/quality/rules.py`](src/pipeline/quality/rules.py).

| Rule | A record fails when |
|---|---|
| `missing_source_id` | it has no `id` (checked at ingestion; the record never reaches bronze) |
| `missing_required_field` | any of `case_number`, `date`, `iucr`, `primary_type`, `arrest`, `domestic`, `year`, `updated_on` is missing |
| `invalid_timestamp` | `date` or `updated_on` can't be parsed, or `date` is in the future |
| `invalid_number` | `id`, `ward`, `community_area` or `year` is present but not a whole number, or a coordinate (`x_coordinate`, `y_coordinate`, `latitude`, `longitude`) is present but not a finite number |
| `invalid_boolean` | `arrest` or `domestic` is present but not a JSON `true` / `false` |
| `year_mismatch` | `year` differs from the year in `date` |
| `coordinates_incomplete` | only one of `latitude` / `longitude` is present |
| `coordinates_out_of_range` | latitude is outside 41.6–42.1, or longitude outside −88.0 to −87.5, i.e. outside the Chicago area |
| `updated_before_date` | `updated_on` is earlier than `date` |
| `model_mismatch` | the record passes every rule above but still doesn't fit the typed `CrimeRecord` model, which means a rule is missing |

How the rules treat values:

- **Missing keys are normal.** The API leaves out fields that are null; a record without coordinates simply has no `latitude` key. So a missing key fails **only** `missing_required_field`, and every other rule skips it. A blank string counts as missing.
- **One problem, one report.** Rules that compare two fields (`year_mismatch`, `updated_before_date`) skip values that don't parse, because the parsing rule already reports them.
- **One quarantine row per broken rule.** A record breaking two rules gets two quarantine rows, but counts as **one** invalid record.
- **Timestamps are Chicago local time.** The API sends them without a timezone.

### The quality gate

Code: [`src/pipeline/quality/gate.py`](src/pipeline/quality/gate.py).

After `validate`, the gate computes the run's failure rate:

```
failure rate = (invalid records + records without an id)
             / (records checked + records without an id)
```

Both sides count records, not rule hits. The rate is based on the records actually checked, not on everything the API returned. Records bronze already had aren't checked again, and pending records left by an earlier, crashed run are checked by this run.

- **Threshold:** `QUALITY_MAX_FAILURE_RATE`, default **0.05** (5%). The run fails only if the rate is **strictly greater** than the threshold, so 1 broken record in 20 (exactly 5%) passes. The comparison uses exact fractions, never floats.
- **Nothing checked:** if a run checked no records, it passes. A run in which every record lacked an `id` has a rate of 100% and fails.
- **On failure:** the decision is saved in `ops.quality_gate_results`, and the run is marked failed. Silver and gold are not touched. Bronze and quarantine keep everything, so the problem can be investigated.

**Why 5%:** on clean data the real failure rate is close to zero (see the results below: 2 broken records in about 1.2 million). The gate isn't meant to catch the occasional bad record; validation already quarantines those one by one. It catches **systematic** breakage, such as an API change that suddenly breaks a large share of records. 5% leaves a wide margin above normal data and still stops a run like that. It assumes **daily** runs of several hundred records. There's no minimum run size, so if runs became much smaller (e.g. hourly, with a few dozen records), a single bad record could exceed 5%, and the threshold should be revisited.

### Held runs and releasing them

Code: [`src/pipeline/quality/release.py`](src/pipeline/quality/release.py).

When a run fails the gate, its **valid** records are **held**: `build_silver` reads only from the view `bronze.promotable_records`, which leaves them out. A later, successful run doesn't publish them either. A run that crashed after `validate` but before the gate is held too, because no one knows whether it would have passed.

After investigating, a person decides what happens to a held run with `release_run(run_id, action, released_by=..., note=...)`:

- **`release`:** the records are fine after all (for example, a handful of bad rows in a small run). The next successful run's `build_silver` publishes them.
- **`recheck`:** a validation rule was missing. After the rule is added, the run's valid records go back to `pending`, so the next run checks them again under the new rules and its gate counts them.

Each held run can be released or rechecked only once. Who did it, when and why is stored next to the gate's decision, which itself is never changed.

### Results on real data

All checks were run on 2026-11-26 against the local database. Every run below passed the quality gate, or was released.

| Data | Records checked | Invalid | Rule hits | Gate |
|---|---:|---:|---|---|
| October 2026 (first run) | 20,125 | 0 | none | checked before the gate existed; released |
| June 2015 (test month) | 23,203 | 0 | none | checked before the gate existed; released |
| Backfill 2022-01-01 to 2026-11-25, one run per month | 1,188,206 | 2 | `coordinates_out_of_range`: 2 | all 59 runs passed |
| **Total in bronze** | **1,231,534** | **2** | | |

- **The two failures:** both are from October 2022 (2 of 23,328 records that month, a rate of 0.009%, the highest of any month). Their blocks are on Congress Pkwy in downtown Chicago, but their coordinates (36.62, −91.69) point to southern Missouri. They are wrong locations, quarantined as they should be.
- **Silver:** holds **1,231,532** records, i.e. every bronze record except the two invalid ones, with no `model_mismatch` rejections.

  | Year | 2015 (June only) | 2022 | 2023 | 2024 | 2025 | 2026 (to 2026-11-18) |
  |---|---:|---:|---:|---:|---:|---:|
  | Records | 23,203 | 240,532 | 263,892 | 259,700 | 238,268 | 205,937 |

  2026 runs only to 18 November because the API publishes records about a week late.
- **Other numbers:** in the backfill, 1,188,206 records were fetched in 27 minutes, with no record missing its `id`. The API returned 503 errors and timeouts 19 times, and every one succeeded on retry. The first silver load took about 6 minutes.

### Independent SQL check

To check the Python rules against something that doesn't share their code, [`docs/sql/independent_quality_check.sql`](docs/sql/independent_quality_check.sql) re-implements the eight payload rules in plain SQL. Timestamps are parsed by PostgreSQL there, not by Python. It then compares the result with what `validate` decided:

- per rule: records flagged by SQL vs quarantine rows written by Python;
- record by record: the SQL verdict vs `bronze.raw_records.validation_status`.

It is read-only and needs PostgreSQL 16 or newer:

```bash
psql -d chicago_pipeline -f docs/sql/independent_quality_check.sql
```

Result on all 1,231,534 bronze records (about 20 seconds):

| Check | SQL | Python | Agree |
|---|---:|---:|---|
| records invalid in both | 2 | 2 | yes |
| records valid in both | 1,231,532 | 1,231,532 | yes |
| `coordinates_out_of_range` | 2 | 2 | yes |
| each of the other 7 rules | 0 | 0 | yes |

On these records, the two never disagree.

Agreement on clean data would also hold for a check that never fires, so the same query was run on the test fixture `tests/fixtures/malformed_batch.json` (20 real records, 7 deliberately broken). It found exactly the 8 expected rule hits, one for each rule, and the same 7 invalid and 13 valid records as the Python rules.

**What this shows, and what it doesn't.** The two agree on the data and the fixture above. That doesn't prove they agree on every possible input, because the timestamp parsers differ:

- Python's `datetime.fromisoformat()` accepts only ISO 8601 strings.
- PostgreSQL's `::timestamp` also accepts other forms, for example `yesterday`, `now`, `epoch`, `infinity`, `06/09/2024` and `June 9 2024`.

On values like these the two can reach different verdicts. For example, for `"date": "yesterday"`, `validate` reports `invalid_timestamp`, while the SQL check reads it as a valid date. Every timestamp the API has sent so far is in ISO form (`2024-06-09T18:30:00.000`), so this hasn't happened on real data. If the two ever disagree, `validate` is the one the pipeline uses.

`missing_source_id` and `model_mismatch` aren't part of this check. The first happens before bronze, and the second needs the Pydantic model.
