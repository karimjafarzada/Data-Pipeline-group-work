"""validate(run_id): check pending bronze rows and quarantine the broken ones.

Every bronze row with validation_status = 'pending' is checked, not only this run's
rows, so rows left behind by a crashed run are picked up too (CLAUDE.md, "Loading
rules"). Rows are handled in batches. Each batch's quarantine rows and new statuses
are saved in ONE transaction, so a row is never marked 'invalid' without its
quarantine rows, or the other way round.

Each checked row also stores validated_run_id, the run that checked it, so that run's
quality gate decides whether the row may reach silver.

Running validate() a second time changes nothing: no row is pending any more, so it
checks nothing and returns zero counts. The quality gate is a separate step that runs
after this one (pipeline/quality/gate.py).
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from pipeline.ingestion.bronze import CHICAGO, Record
from pipeline.ingestion.ingest import restore_autocommit
from pipeline.quality.model import CrimeRecord
from pipeline.quality.rules import Violation, check_record

logger = logging.getLogger(__name__)

# A record passed every rule but still doesn't fit CrimeRecord: a rule is missing.
MODEL_MISMATCH = "model_mismatch"
DEFAULT_BATCH_SIZE = 1000


@dataclass
class ValidationStats:
    checked: int = 0  # pending rows this call looked at
    valid: int = 0  # rows marked 'valid'
    invalid: int = 0  # rows marked 'invalid': distinct records, not rule hits
    rule_hits: dict[str, int] = field(default_factory=dict)  # broken rules, per rule_name


def check_payload(payload: Record, now: datetime) -> list[Violation]:
    """All rule violations for one payload; empty means the record is valid."""
    violations = check_record(payload, now)
    if violations:
        return violations
    try:
        CrimeRecord.model_validate(payload)
    except ValidationError as exc:
        reason = model_error_reason(exc)
        logger.error("Record passed every rule but not CrimeRecord: %s", reason)
        return [Violation(MODEL_MISMATCH, reason)]
    return []


def model_error_reason(exc: ValidationError) -> str:
    """A CrimeRecord error as one line, e.g. 'iucr: Input should be a valid string'."""
    return "; ".join(
        f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
        for error in exc.errors()
    )


def validate(
    run_id: UUID,
    *,
    conn: psycopg.Connection | None = None,
    now: datetime | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> ValidationStats:
    """Mark every pending bronze row 'valid' or 'invalid'; quarantine one row per broken rule.

    ``conn`` and ``now`` are for tests; by default they come from the environment and
    the clock. ``now`` is read once, so every row in this call is judged against the
    same moment. Like ingest(), this switches the connection to autocommit so each
    batch's transaction really commits, and restores a passed-in connection afterwards.
    """
    if now is None:
        now = datetime.now(CHICAGO)
    owns_conn = conn is None
    if conn is None:
        from pipeline.db import get_connection  # Student 3's module, not on main yet

        conn = get_connection()

    stats = ValidationStats()
    rule_hits: Counter[str] = Counter()
    original_autocommit = conn.autocommit
    try:
        conn.autocommit = True
        while _validate_batch(conn, run_id, now, batch_size, stats, rule_hits):
            pass
    finally:
        if owns_conn:
            conn.close()
        else:
            restore_autocommit(conn, original_autocommit)

    stats.rule_hits = dict(sorted(rule_hits.items()))
    logger.info(
        "Validation finished: checked=%d valid=%d invalid=%d rule_hits=%s",
        stats.checked, stats.valid, stats.invalid, stats.rule_hits,
    )
    return stats


def _validate_batch(
    conn: psycopg.Connection,
    run_id: UUID,
    now: datetime,
    batch_size: int,
    stats: ValidationStats,
    rule_hits: Counter[str],
) -> bool:
    """Validate up to batch_size pending rows in one transaction. False when none are left."""
    with conn.transaction():
        # FOR UPDATE SKIP LOCKED: if two validate() calls ever overlap, each row is
        # taken by only one of them. Every row taken is marked below, so it is never
        # pending again and the loop ends.
        rows = conn.execute(
            "SELECT id, endpoint, source_id, payload, payload_hash"
            " FROM bronze.raw_records WHERE validation_status = 'pending'"
            " ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED",
            (batch_size,),
        ).fetchall()
        if not rows:
            return False

        valid_ids: list[int] = []
        invalid_ids: list[int] = []
        quarantine_rows: list[tuple[Any, ...]] = []
        for bronze_id, endpoint, source_id, payload, hash_ in rows:
            violations = check_payload(payload, now)
            if not violations:
                valid_ids.append(bronze_id)
                continue
            invalid_ids.append(bronze_id)
            for violation in violations:
                quarantine_rows.append(
                    (run_id, endpoint, source_id, Jsonb(payload), hash_,
                     violation.rule_name, violation.reason)
                )

        with conn.cursor() as cur:
            if quarantine_rows:
                cur.executemany(
                    "INSERT INTO ops.quarantine"
                    " (run_id, endpoint, source_id, payload, payload_hash, rule_name, reason)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    " ON CONFLICT (payload_hash, rule_name) DO NOTHING",
                    quarantine_rows,
                )
            for status, ids in (("valid", valid_ids), ("invalid", invalid_ids)):
                if ids:
                    # validated_run_id: this run's gate decides whether the row may reach
                    # silver (see bronze.promotable_records).
                    cur.execute(
                        "UPDATE bronze.raw_records"
                        " SET validation_status = %s, validated_run_id = %s"
                        " WHERE id = ANY(%s)",
                        (status, run_id, ids),
                    )

    # Count only after the transaction has committed.
    stats.checked += len(rows)
    stats.valid += len(valid_ids)
    stats.invalid += len(invalid_ids)
    rule_hits.update(row[5] for row in quarantine_rows)
    logger.info(
        "Validated batch: checked=%d valid=%d invalid=%d",
        len(rows), len(valid_ids), len(invalid_ids),
    )
    return True
