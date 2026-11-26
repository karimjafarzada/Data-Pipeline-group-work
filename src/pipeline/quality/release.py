"""release_run(): what a person does after investigating a run the quality gate held.

A run's valid rows are held, meaning they're not in bronze.promotable_records and so
can't reach silver, when the run failed the quality gate, or never reached the gate
because it crashed after validate(). After investigating, a person chooses one of:

- release: the rows are fine after all. The next successful run's build_silver()
           promotes them.
- recheck: a rule was missing. After adding the rule, reset the run's valid rows to
           'pending', so the next run's validate() checks them again under the new
           rules, and that run's gate counts them.

Each held run can be released or rechecked once. Who did it, when and why is kept in
ops.quality_gate_results, next to the gate's decision (which is never changed).
"""

from __future__ import annotations

import logging
from uuid import UUID

import psycopg

logger = logging.getLogger(__name__)

RELEASE = "release"
RECHECK = "recheck"
ACTIONS = (RELEASE, RECHECK)
_PAST_TENSE = {RELEASE: "released", RECHECK: "rechecked"}


class ReleaseError(Exception):
    """The run can't be released or rechecked; the message says why."""


def release_run(
    run_id: UUID,
    action: str,
    *,
    released_by: str,
    note: str,
    conn: psycopg.Connection | None = None,
) -> int:
    """Release or recheck a held run's valid rows. Returns how many rows that was.

    Everything happens in one transaction: if anything fails, nothing changes. Raises
    ReleaseError if the run passed the gate, was already released or rechecked, or has
    no held rows. ``conn`` is for tests; by default it comes from the environment.
    """
    if action not in ACTIONS:
        raise ValueError(f"action must be one of {ACTIONS}, got {action!r}")
    if not released_by.strip() or not note.strip():
        raise ValueError("released_by and note are required: say who released the run and why")

    owns_conn = conn is None
    if conn is None:
        from pipeline.db import get_connection  # Student 3's module, not on main yet

        conn = get_connection()
    try:
        with conn.transaction():
            rows = _release(conn, run_id, action, released_by.strip(), note.strip())
    finally:
        if owns_conn:
            conn.close()

    logger.info(
        "Run %s %s by %s: %d row(s). Note: %s",
        run_id, _PAST_TENSE[action], released_by, rows, note,
    )
    return rows


def _release(
    conn: psycopg.Connection, run_id: UUID, action: str, released_by: str, note: str
) -> int:
    # FOR UPDATE: a second release_run() for the same run waits here, then sees ours.
    gate = conn.execute(
        "SELECT passed, release_action, released_by, released_at"
        " FROM ops.quality_gate_results WHERE run_id = %s FOR UPDATE",
        (run_id,),
    ).fetchone()
    if gate is not None:
        passed, done, done_by, done_at = gate
        if passed:
            raise ReleaseError(f"Run {run_id} passed the quality gate: none of its rows are held")
        if done is not None:
            raise ReleaseError(
                f"Run {run_id} was already {_PAST_TENSE[done]} by {done_by} at {done_at}"
            )
    # gate is None: the run crashed after validate() and never reached the gate.

    held = conn.execute(
        "SELECT count(*) FROM bronze.raw_records"
        " WHERE validated_run_id = %s AND validation_status = 'valid'",
        (run_id,),
    ).fetchone()[0]
    if held == 0:
        raise ReleaseError(f"Run {run_id} has no held rows: it marked no bronze rows valid")

    if action == RECHECK:
        conn.execute(
            "UPDATE bronze.raw_records SET validation_status = 'pending', validated_run_id = NULL"
            " WHERE validated_run_id = %s AND validation_status = 'valid'",
            (run_id,),
        )

    # Inserts a row when the run never reached the gate. The WHERE guards against a
    # concurrent release_run() that inserted that row after our SELECT found none.
    saved = conn.execute(
        "INSERT INTO ops.quality_gate_results AS g"
        " (run_id, release_action, released_by, release_note, released_at)"
        " VALUES (%s, %s, %s, %s, now())"
        " ON CONFLICT (run_id) DO UPDATE SET"
        " release_action = EXCLUDED.release_action, released_by = EXCLUDED.released_by,"
        " release_note = EXCLUDED.release_note, released_at = EXCLUDED.released_at"
        " WHERE g.release_action IS NULL AND g.passed IS NOT TRUE",
        (run_id, action, released_by, note),
    ).rowcount
    if saved != 1:
        raise ReleaseError(f"Run {run_id} was released or rechecked at the same time; try again")
    return held
