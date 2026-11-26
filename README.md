# Data Pipeline Group Work

This Python pipeline loads Chicago Crimes records into PostgreSQL. Bronze stores the API payloads, silver holds validated records, and gold serves analytical SQL. See [the shared data contracts](CLAUDE.md) for table definitions and validation rules.

| Member | Primary responsibility |
|---|---|
| Amin Rasulzada (@aminrasulzada) | API client, checkpoints, validation, and silver |
| Talib Abilov (@talib-abilov) | Gold, analytics, and performance |
| Karim Jafar-zada (@karimjafarzada) | Docker, CI, orchestration, run tracking, and recovery |

## How to run

Start Docker Desktop or Docker Engine with Compose v2. Copy `.env.example` to `.env`, set a local `POSTGRES_PASSWORD`, and optionally set `SOCRATA_APP_TOKEN`. The host uses `127.0.0.1:5433` by default. Containers use `db:5432`. Keep `.env` local.

A PostgreSQL 16 volume needs migration before PostgreSQL 17 can use it. To keep existing data, back it up with PostgreSQL 16 and restore it into a new PostgreSQL 17 volume. For a disposable local database, `docker compose down --volumes` deletes its data so the next startup can initialize a fresh volume.

```sh
docker compose up -d --wait db
docker compose build pipeline
docker compose run --rm pipeline python -m pipeline.migrate
```

Run a small backfill before the first incremental run. Date bounds include both endpoints. Use the same date range again to resume an interrupted backfill.

```sh
docker compose run --rm pipeline python -m pipeline run --mode backfill --start 2025-01-01 --end 2025-01-01
docker compose run --rm pipeline python -m pipeline run --mode incremental
```

The runner registers each attempt in `ops.pipeline_runs`, fetches and validates data, evaluates the quality threshold, and then calls the silver and gold loaders. End-to-end runs require the silver and gold loaders.

```sh
docker compose exec db psql -U warehouse -d warehouse -c "SELECT run_id, mode, status, rows_fetched, rows_valid, rows_quarantined, error_message FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 10;"
```

The default quality threshold is 5%. Review `ops.quarantine` and `ops.quality_gate_results` before releasing a failed run's valid rows. A failed quality gate exits with code 2. Use `--action release` to allow valid rows to be promoted on the next successful run, or `--action recheck` to return them to validation. Both actions require the investigator's name and a note.

```sh
docker compose run --rm pipeline python -m pipeline release --run-id YOUR_RUN_UUID --released-by YOUR_NAME --action recheck --note "Explain what was investigated"
```

## Daily schedule

After the initial backfill, start the scheduler. It starts an incremental run immediately on each start or restart, then repeats every 24 hours while the container is running. The interval is measured from startup, so it can shift with restarts.

```sh
docker compose --profile scheduled up -d scheduler
docker compose logs -f scheduler
```

Set `SCHEDULE_INTERVAL_SECONDS` to a shorter interval for a local demo and recreate the scheduler. Manual and scheduled runs share a database lock. Use `docker compose --profile scheduled down` to stop services and retain the named database volume.

## Recovery demonstrations

From a Bash environment with Docker Compose and OpenSSL, run:

```sh
bash scripts/recovery_demo.sh
```

The script builds the worker, creates a disposable Compose project, and uses fixture-backed API pages.

| Case | What the demonstration checks |
|---|---|
| PostgreSQL unavailable at startup | The migration container logs a retry while the database is down, then creates the run-history table once Postgres starts. |
| Ingestion container killed | The worker is killed after committing page 1. The replacement CLI marks the interrupted run failed, reloads the stored cursor, and inserts page 2. The script checks that IDs 1 and 2 appear once and the final cursor is 2. |
| Malformed batch | The real CLI ingests `tests/fixtures/malformed_batch.json`. The run exits with code 2 and stops before either loader runs. The script checks counts and quarantine entries against the fixture's expected values. |

See the [GitHub Actions workflow](https://github.com/karimjafarzada/Data-Pipeline-group-work/actions/workflows/postgres-smoke.yml) for current recovery, lint, and integration results.

## Run tests locally

```sh
python -m pip install -e '.[dev]'
ruff check src tests
python -m pytest -ra
```

For integration tests, start the Compose database and export `POSTGRES_HOST=127.0.0.1`, `POSTGRES_PORT=5433`, `POSTGRES_USER=warehouse`, `POSTGRES_DB=warehouse`, and the local `POSTGRES_PASSWORD` in the terminal running pytest. Use the published port if you changed it. Tests create and remove temporary databases. PostgreSQL tests are skipped locally when the database is unavailable and fail when `CI` is set.

## Reviews and CI

Use a feature branch for each change and get a teammate's approval before merging a PR into `main`. Keep commits small and use Conventional Commits. The workflow checks Docker startup, Ruff lint, migrations, and pytest with PostgreSQL. Install the development extras and run `pre-commit install` to enable the local lint hook.
