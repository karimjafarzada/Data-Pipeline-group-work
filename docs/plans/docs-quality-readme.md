# Plan: docs/quality-readme (README section on data quality)

Written 2026-11-26 while the team lead was away. The plan was not approved before implementation; review it together with the text.

## Goal

A "Data quality" section in `README.md` that a reader new to the project can follow without reading the code:

1. **Every validation rule:** what fails it, and the conventions (a missing key fails only the required-field rule, a blank string counts as missing, one quarantine row per rule per record).
2. **The quality gate:** the formula, the threshold (0.05) and why, "exceeds" meaning strictly greater, the zero-rows case, and the assumption of daily runs.
3. **Held runs and `release_run`:** why a failed run's valid rows stay out of silver, and the `release` and `recheck` actions.
4. **Results on real data:**
   - October 2026: 20,125 records + 13 added later, 0 failures.
   - June 2015: 23,203 records, 0 failures.
   - The full 2022-01 to 2026-11 backfill: 1,188,206 records, 2 failures (`coordinates_out_of_range`, October 2022), every month passing the gate.
   - The silver load: 1,231,532 rows.
5. **An independent SQL check:** the rules re-implemented in plain SQL, run over every bronze payload, and compared with what `validate()` decided (per-rule hit counts, and valid/invalid record by record).

## Approach

- **The SQL check** goes in `docs/sql/independent_quality_check.sql`, so anyone can re-run it with `psql -f`. It is read-only: one SELECT statement that writes nothing. It uses `pg_input_is_valid()`, so it needs PostgreSQL 16 or newer. `model_mismatch` and `missing_source_id` can't be checked this way: the first needs the Pydantic model, and the second is checked before bronze. The README says so.
- **Numbers** come from the local `chicago_pipeline` database (`ops.pipeline_runs`, `ops.quarantine`, `ops.quality_gate_results`) and the backfill log, read on 2026-11-26.
- **README:** keep the existing heading and add the section under it. Team members and the Docker instructions belong to Student 3, so I don't add them.
- **Scope:** documentation only. No code changes.

## Commits

1. `docs: plan the data quality README section`
2. `docs: add an independent SQL check of the validation rules`
3. `docs: describe data quality rules, the gate and real-data results in README`
