"""Command-line entry point for one pipeline run or a reviewed quality release."""

import argparse
import logging
from datetime import date
from uuid import UUID

from pipeline.db import get_connection
from pipeline.ingestion.ingest import ingest
from pipeline.migrate import apply_migrations
from pipeline.quality.gate import QualityGateError, run_quality_gate
from pipeline.quality.release import ACTIONS, RELEASE, ReleaseError, release_run
from pipeline.quality.validate import validate
from pipeline.runs import RunCounts, finish_run, start_run

logger = logging.getLogger(__name__)
RUN_LOCK_ID = 7_420_002


def _publication_steps():
    from pipeline.gold import load_gold
    from pipeline.silver.build import build_silver

    return build_silver, load_gold


def run_pipeline(mode: str, start: date | None = None, end: date | None = None) -> UUID:
    build_silver, load_gold = _publication_steps()
    with get_connection() as conn:
        conn.autocommit = True
        if not conn.execute("SELECT pg_try_advisory_lock(%s)", (RUN_LOCK_ID,)).fetchone()[0]:
            raise RuntimeError("A pipeline run or release is already in progress")
        # The session-level lock is released when this connection closes, including on SIGKILL.
        apply_migrations(conn)
        conn.execute(
            """UPDATE ops.pipeline_runs
               SET status = 'failed', finished_at = clock_timestamp(),
                   error_message = 'Previous pipeline process exited before finishing'
               WHERE status = 'running'"""
        )
        run_id = start_run(mode, start, end, conn=conn)
        logger.info("Started %s run %s", mode, run_id)
        counts = RunCounts()
        try:
            ingested = ingest(run_id, mode, start, end, conn=conn)
            counts = RunCounts(ingested.rows_fetched, 0, ingested.rows_quarantined)
            validated = validate(run_id, conn=conn)
            counts = RunCounts(
                ingested.rows_fetched, validated.valid,
                validated.invalid + ingested.rows_quarantined,
            )
            run_quality_gate(run_id, ingested, validated, conn=conn)
            build_silver(run_id)
            load_gold(run_id)
            finish_run(run_id, "success", counts, conn=conn)
        except Exception as error:
            # Retain the original failure if the database is also lost while recording it.
            try:
                finish_run(run_id, "failed", counts, str(error), conn=conn)
            except Exception:
                logger.exception("Could not record failure for run %s", run_id)
            raise
    logger.info("Run %s succeeded", run_id)
    return run_id


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Run the full pipeline once")
    run.add_argument("--mode", choices=("incremental", "backfill"), required=True)
    run.add_argument("--start", type=date.fromisoformat, help="Inclusive backfill start date")
    run.add_argument("--end", type=date.fromisoformat, help="Inclusive backfill end date")

    release = commands.add_parser("release", help="Release or recheck held records")
    release.add_argument("--run-id", type=UUID, required=True)
    release.add_argument("--note", required=True)
    release.add_argument("--released-by", required=True, help="Name of the investigator")
    release.add_argument("--action", choices=ACTIONS, default=RELEASE)

    args = parser.parse_args(argv)
    if args.command == "run":
        if args.mode == "backfill" and not (args.start and args.end and args.start <= args.end):
            parser.error("Backfill requires --start and --end in chronological order")
        if args.mode == "incremental" and (args.start or args.end):
            parser.error("--start and --end are only supported for backfills")

    try:
        if args.command == "run":
            run_pipeline(args.mode, args.start, args.end)
        else:
            with get_connection() as conn:
                conn.autocommit = True
                if not conn.execute(
                    "SELECT pg_try_advisory_lock(%s)", (RUN_LOCK_ID,)
                ).fetchone()[0]:
                    raise RuntimeError("A pipeline run or release is already in progress")
                count = release_run(
                    args.run_id, args.action, released_by=args.released_by, note=args.note, conn=conn
                )
            if args.action == RELEASE:
                logger.info("Released %s rows for promotion by the next successful run", count)
            else:
                logger.info("Queued %s rows for validation on the next run", count)
    except QualityGateError as error:
        logger.error("%s", error)
        return 2
    except (ReleaseError, ValueError) as error:
        logger.error("%s", error)
        return 1
    except Exception:
        logger.exception("Pipeline command failed")
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main())
