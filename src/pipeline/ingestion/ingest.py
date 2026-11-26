"""Resumable ingestion: API pages -> bronze.raw_records.

Each page is written to bronze and its checkpoint is saved in ONE transaction. After a
crash, bronze and the checkpoint therefore always agree, and the next run continues from
the checkpoint (see CLAUDE.md, "Loading rules").

- backfill:    pages by id inside a date range; checkpoint = highest id written.
- incremental: pages by (updated_on, id), so it finds new records AND records changed
               since the last run; checkpoint = '<updated_on>|<id>' of the last record.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from uuid import UUID

import psycopg

from pipeline.ingestion.bronze import CHICAGO, Record, write_page
from pipeline.ingestion.checkpoint import (
    checkpoint_key,
    format_change_cursor,
    load_checkpoint,
    parse_change_cursor,
    save_checkpoint,
)
from pipeline.ingestion.client import DATASET_ID, SocrataClient, page_last_change, page_max_id

logger = logging.getLogger(__name__)

Page = list[Record]


class IngestionError(Exception):
    """Ingestion can't start or continue (e.g. nothing to resume from, or already running)."""


@dataclass
class IngestStats:
    pages: int = 0
    rows_fetched: int = 0  # every record the API returned, including ones bronze already had
    rows_inserted: int = 0  # new rows in bronze
    rows_duplicate: int = 0  # records bronze already had
    rows_quarantined: int = 0  # records without an id, sent to quarantine
    cursor: str | None = None  # the checkpoint when the run ended


def ingest(
    run_id: UUID,
    mode: str,
    start: date | None = None,
    end: date | None = None,
    *,
    conn: psycopg.Connection | None = None,
    client: SocrataClient | None = None,
) -> IngestStats:
    """Fetch pages from the API into bronze, continuing from the saved checkpoint.

    ``conn`` and ``client`` are for tests; by default they come from the environment.
    While ingesting, the connection is switched to autocommit, so each page's
    ``conn.transaction()`` really commits on its own (psycopg refuses if a transaction
    is already open). A passed-in connection gets its original setting back afterwards,
    also when ingestion raises.
    """
    key = checkpoint_key(mode, DATASET_ID, start, end)  # checks mode and dates
    if start is not None and end is not None and start > end:
        raise ValueError(f"start {start} is after end {end}")

    if client is None:
        client = SocrataClient.from_env()
    owns_conn = conn is None
    if conn is None:
        from pipeline.db import get_connection  # Student 3's module, not on main yet

        conn = get_connection()

    original_autocommit = conn.autocommit
    try:
        conn.autocommit = True
        with _ingestion_lock(conn, key):
            return _ingest_pages(conn, client, run_id, mode, key, start, end)
    finally:
        if owns_conn:
            conn.close()
        else:
            restore_autocommit(conn, original_autocommit)


def restore_autocommit(conn: psycopg.Connection, value: bool) -> None:
    if conn.closed or conn.autocommit == value:
        return
    try:
        conn.autocommit = value
    except psycopg.Error:
        # Only possible if the connection is broken; don't hide the original error.
        logger.warning("Could not restore autocommit=%s on the connection", value)


def _ingest_pages(
    conn: psycopg.Connection,
    client: SocrataClient,
    run_id: UUID,
    mode: str,
    key: str,
    start: date | None,
    end: date | None,
) -> IngestStats:
    cursor = load_checkpoint(conn, key)
    if mode == "backfill":
        pages, cursor_after = _backfill_pages(client, start, end, cursor)
    else:
        pages, cursor_after = _incremental_pages(conn, client, cursor)
    logger.info("Ingesting %s from checkpoint %s", key, cursor)

    stats = IngestStats(cursor=cursor)
    for page in pages:  # the API call for the next page happens outside any transaction
        new_cursor = cursor_after(page)
        with conn.transaction():  # page and checkpoint: both saved, or neither
            result = write_page(conn, run_id, DATASET_ID, page)
            save_checkpoint(conn, key, new_cursor, run_id)

        stats.pages += 1
        stats.rows_fetched += len(page)
        stats.rows_inserted += result.inserted
        stats.rows_duplicate += result.duplicates
        stats.rows_quarantined += result.quarantined
        stats.cursor = new_cursor
        logger.info(
            "Saved page %d: fetched=%d inserted=%d duplicates=%d quarantined=%d checkpoint=%s",
            stats.pages, len(page), result.inserted, result.duplicates, result.quarantined,
            new_cursor,
        )

    logger.info("Ingestion of %s finished: %s", key, stats)
    return stats


def _backfill_pages(
    client: SocrataClient, start: date | None, end: date | None, cursor: str | None
) -> tuple[Iterator[Page], Callable[[Page], str]]:
    after_id = int(cursor) if cursor is not None else None
    pages = client.iter_pages(start_date=start, end_date=end, after_id=after_id)
    return pages, lambda page: str(page_max_id(page))


def _incremental_pages(
    conn: psycopg.Connection, client: SocrataClient, cursor: str | None
) -> tuple[Iterator[Page], Callable[[Page], str]]:
    if cursor is None:
        cursor = _start_from_bronze(conn)
    updated_after, after_id = parse_change_cursor(cursor)
    pages = client.iter_changed_pages(updated_after, after_id)
    return pages, lambda page: format_change_cursor(*page_last_change(page))


def _start_from_bronze(conn: psycopg.Connection) -> str:
    """First incremental run: start from the newest change already in bronze."""
    newest = conn.execute(
        "SELECT max(source_updated_at) FROM bronze.raw_records WHERE endpoint = %s",
        (DATASET_ID,),
    ).fetchone()[0]
    if newest is None:
        raise IngestionError(
            "No incremental checkpoint and bronze is empty: run a backfill first"
        )
    # Back to the API's format: Chicago local time with milliseconds, no timezone.
    local = newest.astimezone(CHICAGO)
    timestamp = local.strftime("%Y-%m-%dT%H:%M:%S.") + f"{local.microsecond // 1000:03d}"
    logger.info("No incremental checkpoint yet: starting from %s, the newest in bronze", timestamp)
    # id 0 means "every record at that timestamp"; ones bronze already has are skipped.
    return format_change_cursor(timestamp, 0)


@contextmanager
def _ingestion_lock(conn: psycopg.Connection, key: str) -> Iterator[None]:
    """Stop two runs from ingesting the same checkpoint key at the same time."""
    locked = conn.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (key,)).fetchone()[0]
    if not locked:
        raise IngestionError(f"Another run is already ingesting {key}")
    try:
        yield
    finally:
        try:
            conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (key,))
        except psycopg.Error:
            # The connection is broken; Postgres frees the lock when it closes.
            logger.warning("Could not release the ingestion lock for %s", key)
