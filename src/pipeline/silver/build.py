"""build_silver(run_id): promote the newest promotable bronze version of each record.

Runs after run_quality_gate() (CLAUDE.md, "run_id"). It reads only
bronze.promotable_records: valid rows whose validating run passed the gate or was
released. Of those, only rows no earlier build has handled (silver_run_id IS NULL) are
read, so an incremental run reads only what is new, through a small partial index.

Everything happens in ONE transaction:

1. Snapshot the candidates (promotable rows not handled yet) into a temp table. Every
   later step uses this snapshot, so a row released while the build runs is never
   marked handled without having been looked at.
2. Pick the newest candidate per source_id: by source_updated_at, then bronze id.
3. Turn each payload into a CrimeRecord and COPY the rows into a temp table, a batch at
   a time. COPY is far faster than row-by-row INSERTs for ~1.2M rows.
4. Upsert the temp table into silver.crimes in one statement. A row is replaced only
   by a strictly newer version, (source_updated_at, bronze_id), so an older version
   never overwrites a newer one and a re-run never rewrites a row with itself.
5. Mark every candidate as handled (silver_run_id = this run), winners and losers alike.

A payload that no longer fits CrimeRecord (a rule or the model changed after it was
validated) is not skipped silently: it is quarantined as 'model_mismatch', its bronze
row becomes 'invalid', it is counted in SilverStats.rejected, and the record's next
newest candidate (if any) takes its place. Silver keeps the last valid version.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from pipeline.ingestion.ingest import restore_autocommit
from pipeline.quality.model import CrimeRecord
from pipeline.quality.validate import MODEL_MISMATCH, model_error_reason

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 10_000
# Same order as the silver.crimes columns (a migration test checks this).
SILVER_COLUMNS = (*CrimeRecord.model_fields, "bronze_id", "run_id")


class SilverBuildError(Exception):
    """build_silver() can't run, e.g. another build is already running."""


@dataclass
class SilverStats:
    candidates: int = 0  # promotable bronze rows no earlier build had handled
    records: int = 0  # distinct records (source_ids) among them
    inserted: int = 0  # records new to silver
    updated: int = 0  # records whose silver row was replaced by a newer version
    unchanged: int = 0  # records where silver already had this version or a newer one
    rejected: int = 0  # newest versions that failed CrimeRecord (quarantined, now invalid)
    seconds: float = 0.0


@dataclass(frozen=True)
class _Rejected:
    bronze_id: int
    endpoint: str
    source_id: str
    payload: dict[str, Any]
    payload_hash: str
    reason: str


def build_silver(
    run_id: UUID,
    *,
    conn: psycopg.Connection | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> SilverStats:
    """Upsert the newest promotable version of every record not handled yet into silver.

    ``conn`` is for tests; by default it comes from the environment. Like validate(),
    this switches the connection to autocommit so the build's transaction really
    commits, and restores a passed-in connection afterwards.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    started = time.monotonic()
    owns_conn = conn is None
    if conn is None:
        from pipeline.db import get_connection  # Student 3's module, not on main yet

        conn = get_connection()

    original_autocommit = conn.autocommit
    try:
        conn.autocommit = True
        with conn.transaction():
            stats = _build(conn, run_id, batch_size)
    finally:
        if owns_conn:
            conn.close()
        else:
            restore_autocommit(conn, original_autocommit)

    stats.seconds = round(time.monotonic() - started, 1)
    logger.info("Silver build finished: %s", stats)
    return stats


def _build(conn: psycopg.Connection, run_id: UUID, batch_size: int) -> SilverStats:
    # Released automatically when the transaction ends, also on errors.
    locked = conn.execute(
        "SELECT pg_try_advisory_xact_lock(hashtext('build_silver'))"
    ).fetchone()[0]
    if not locked:
        raise SilverBuildError("Another build_silver() is already running")

    stats = SilverStats()
    # ON COMMIT DROP: the temp tables disappear with the transaction.
    conn.execute(
        "CREATE TEMP TABLE silver_candidates ON COMMIT DROP AS"
        " SELECT id, source_id, source_updated_at FROM bronze.promotable_records"
        " WHERE silver_run_id IS NULL"
    )
    conn.execute("ANALYZE silver_candidates")  # temp tables get no statistics otherwise
    stats.candidates = conn.execute("SELECT count(*) FROM silver_candidates").fetchone()[0]
    if stats.candidates == 0:
        return stats

    # LIKE copies the columns, types and NOT NULLs, but not the keys.
    conn.execute("CREATE TEMP TABLE silver_stage (LIKE silver.crimes) ON COMMIT DROP")
    conn.execute("CREATE TEMP TABLE silver_winners (id BIGINT) ON COMMIT DROP")

    # Round 1 takes every record. A rejected winner is removed from the candidates, and
    # the next round looks again at just those records, so an older valid version can
    # take its place. Each round removes at least one candidate, so this ends.
    source_ids: list[str] | None = None
    while True:
        rejected = _stage_newest(conn, run_id, batch_size, source_ids, stats)
        if not rejected:
            break
        _reject(conn, run_id, rejected)
        stats.rejected += len(rejected)
        source_ids = sorted({r.source_id for r in rejected})

    staged = conn.execute("SELECT count(*) FROM silver_stage").fetchone()[0]
    already_in_silver = conn.execute(
        "SELECT count(*) FROM silver_stage s JOIN silver.crimes c ON c.id = s.id"
    ).fetchone()[0]
    written = conn.execute(_UPSERT).rowcount  # inserted + updated
    stats.inserted = staged - already_in_silver
    stats.updated = written - stats.inserted
    stats.unchanged = staged - written

    conn.execute(
        "UPDATE bronze.raw_records AS r SET silver_run_id = %s"
        " FROM silver_candidates AS c WHERE r.id = c.id",
        (run_id,),
    )
    return stats


def _stage_newest(
    conn: psycopg.Connection,
    run_id: UUID,
    batch_size: int,
    source_ids: list[str] | None,
    stats: SilverStats,
) -> list[_Rejected]:
    """COPY the newest candidate of each record into silver_stage; return the rejects."""
    # Pick the winners from the small candidates table first, so the sort doesn't drag
    # the large payloads along; then fetch only the winners' payloads. (INSERT, not
    # CREATE TABLE AS: Postgres takes query parameters only in ordinary DML.)
    conn.execute("TRUNCATE silver_winners")
    conn.execute(
        "INSERT INTO silver_winners"
        " SELECT DISTINCT ON (source_id) id FROM silver_candidates"
        " WHERE %s::text[] IS NULL OR source_id = ANY(%s::text[])"
        " ORDER BY source_id, source_updated_at DESC, id DESC",
        (source_ids, source_ids),
    )
    if source_ids is None:
        stats.records = conn.execute("SELECT count(*) FROM silver_winners").fetchone()[0]

    rejected: list[_Rejected] = []
    copy_sql = sql.SQL("COPY silver_stage ({}) FROM STDIN").format(
        sql.SQL(", ").join(map(sql.Identifier, SILVER_COLUMNS))
    )
    # A named (server-side) cursor streams the payloads instead of loading them all.
    with conn.cursor(name="silver_winner_payloads") as winners:
        winners.execute(
            "SELECT r.id, r.endpoint, r.source_id, r.payload, r.payload_hash"
            " FROM silver_winners AS w JOIN bronze.raw_records AS r ON r.id = w.id"
        )
        while batch := winners.fetchmany(batch_size):
            rows = list(_silver_rows(batch, run_id, rejected))
            # Between two fetches the connection is free, so a COPY can run.
            with conn.cursor() as cur, cur.copy(copy_sql) as copy:
                for row in rows:
                    copy.write_row(row)
    return rejected


def _silver_rows(
    batch: Iterable[tuple[Any, ...]], run_id: UUID, rejected: list[_Rejected]
) -> Iterable[tuple[Any, ...]]:
    for bronze_id, endpoint, source_id, payload, hash_ in batch:
        try:
            record = CrimeRecord.model_validate(payload)
        except ValidationError as exc:
            rejected.append(
                _Rejected(bronze_id, endpoint, source_id, payload, hash_, model_error_reason(exc))
            )
            continue
        yield (*record.model_dump().values(), bronze_id, run_id)


def _reject(conn: psycopg.Connection, run_id: UUID, rejected: list[_Rejected]) -> None:
    for r in rejected:
        logger.error(
            "Bronze row %d (record %s) is promotable but doesn't fit CrimeRecord: %s",
            r.bronze_id, r.source_id, r.reason,
        )
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO ops.quarantine"
            " (run_id, endpoint, source_id, payload, payload_hash, rule_name, reason)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (payload_hash, rule_name) DO NOTHING",
            [(run_id, r.endpoint, r.source_id, Jsonb(r.payload), r.payload_hash,
              MODEL_MISMATCH, r.reason) for r in rejected],
        )
    ids = [r.bronze_id for r in rejected]
    conn.execute(
        "UPDATE bronze.raw_records SET validation_status = 'invalid' WHERE id = ANY(%s)", (ids,)
    )
    conn.execute("DELETE FROM silver_candidates WHERE id = ANY(%s)", (ids,))


# Replace a silver row only with a strictly newer version: newest by source_updated_at,
# then by bronze id (the same order used to pick the winners). Every column except id is
# overwritten, so run_id records the run that last wrote the row.
_UPSERT = sql.SQL(
    "INSERT INTO silver.crimes ({columns}) SELECT {columns} FROM silver_stage"
    " ON CONFLICT (id) DO UPDATE SET {updates}"
    " WHERE (silver.crimes.source_updated_at, silver.crimes.bronze_id)"
    " < (EXCLUDED.source_updated_at, EXCLUDED.bronze_id)"
).format(
    columns=sql.SQL(", ").join(map(sql.Identifier, SILVER_COLUMNS)),
    updates=sql.SQL(", ").join(
        sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(column))
        for column in SILVER_COLUMNS
        if column != "id"
    ),
)
