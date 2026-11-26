"""Record the start and outcome of an execution in ops.pipeline_runs."""

from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date
from uuid import UUID, uuid4

import psycopg

from pipeline.db import get_connection


@dataclass(frozen=True)
class RunCounts:
    rows_fetched: int = 0
    rows_valid: int = 0
    rows_quarantined: int = 0


def start_run(
    mode: str,
    start: date | None = None,
    end: date | None = None,
    *,
    conn: psycopg.Connection | None = None,
) -> UUID:
    """Create and return a run ID before invoking any pipeline steps."""
    if mode not in {"incremental", "backfill"}:
        raise ValueError("Run mode must be incremental or backfill")
    if mode == "backfill" and (start is None or end is None or start > end):
        raise ValueError("Backfill requires a start date on or before the end date")
    if mode == "incremental" and (start is not None or end is not None):
        raise ValueError("Date bounds are only supported for backfills")

    run_id = uuid4()
    with (get_connection() if conn is None else nullcontext(conn)) as db, db.transaction():
        db.execute(
            """INSERT INTO ops.pipeline_runs
               (run_id, mode, backfill_start, backfill_end, status)
               VALUES (%s, %s, %s, %s, 'running')""",
            (run_id, mode, start, end),
        )
    return run_id


def finish_run(
    run_id: UUID,
    status: str,
    counts: RunCounts,
    error: str | None = None,
    *,
    conn: psycopg.Connection | None = None,
) -> None:
    """Set the final status and counters without overwriting a completed run."""
    if status not in {"success", "failed"}:
        raise ValueError("Final run status must be success or failed")
    counters = (counts.rows_fetched, counts.rows_valid, counts.rows_quarantined)
    if any(count < 0 for count in counters):
        raise ValueError("Run counts cannot be negative")

    with (get_connection() if conn is None else nullcontext(conn)) as db, db.transaction():
        updated = db.execute(
            """UPDATE ops.pipeline_runs
               SET status = %s, finished_at = clock_timestamp(),
                   rows_fetched = %s, rows_valid = %s, rows_quarantined = %s,
                   error_message = %s
               WHERE run_id = %s AND status = 'running'""",
            (status, *counters, error, run_id),
        )
        if updated.rowcount != 1:
            raise ValueError(f"Run {run_id} does not exist or has already finished")
