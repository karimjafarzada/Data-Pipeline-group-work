"""Write pages of API records to bronze.raw_records.

Records without an ``id`` can't go to bronze (``source_id`` is NOT NULL), so they go
straight to ops.quarantine with rule ``missing_source_id`` (see CLAUDE.md, "Loading rules").
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

# The API's timestamps have no timezone; they are Chicago local time.
CHICAGO = ZoneInfo("America/Chicago")
MISSING_SOURCE_ID_RULE = "missing_source_id"

Record = dict[str, Any]


@dataclass(frozen=True)
class PageWriteResult:
    inserted: int  # new rows in bronze
    duplicates: int  # records bronze already had (same id and same hash)
    quarantined: int  # records without an id, newly written to quarantine


def payload_hash(record: Record) -> str:
    """sha256 of the record as JSON with sorted keys, so key order never matters."""
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_source_timestamp(value: Any) -> datetime | None:
    """Turn an API timestamp like '2024-01-31T14:00:00.000' into a timezone-aware datetime.

    Returns None when the value is missing or unreadable. Bronze keeps the raw payload
    either way; validation reports bad values later.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        # In the autumn clock change one hour happens twice; this picks the first one.
        parsed = parsed.replace(tzinfo=CHICAGO)
    return parsed


def has_source_id(record: Record) -> bool:
    value = record.get("id")
    return value is not None and str(value).strip() != ""


def write_page(
    conn: psycopg.Connection, run_id: UUID, endpoint: str, records: Sequence[Record]
) -> PageWriteResult:
    """Write one page. It does not commit: the caller owns the transaction."""
    with_id = [r for r in records if has_source_id(r)]
    without_id = [r for r in records if not has_source_id(r)]

    inserted = _insert_bronze(conn, run_id, endpoint, with_id)
    quarantined = _quarantine_missing_ids(conn, run_id, endpoint, without_id)
    return PageWriteResult(
        inserted=inserted, duplicates=len(with_id) - inserted, quarantined=quarantined
    )


def _insert_bronze(
    conn: psycopg.Connection, run_id: UUID, endpoint: str, records: list[Record]
) -> int:
    if not records:
        return 0
    rows = [
        (
            endpoint,
            str(record["id"]),
            Jsonb(record),
            payload_hash(record),
            parse_source_timestamp(record.get("updated_on")),
            run_id,
        )
        for record in records
    ]
    with conn.cursor() as cur:
        # Same id and same hash = a version we already have: skip it.
        # A changed record has a new hash, so it gets a new row (history for SCD2).
        cur.executemany(
            "INSERT INTO bronze.raw_records"
            " (endpoint, source_id, payload, payload_hash, source_updated_at, run_id)"
            " VALUES (%s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (endpoint, source_id, payload_hash) DO NOTHING",
            rows,
        )
        return cur.rowcount  # rows actually inserted, summed over all records


def _quarantine_missing_ids(
    conn: psycopg.Connection, run_id: UUID, endpoint: str, records: list[Record]
) -> int:
    if not records:
        return 0
    rows = [
        (
            run_id,
            endpoint,
            Jsonb(record),
            payload_hash(record),
            MISSING_SOURCE_ID_RULE,
            "Record has no id, so it can't be stored in bronze",
        )
        for record in records
    ]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO ops.quarantine"
            " (run_id, endpoint, source_id, payload, payload_hash, rule_name, reason)"
            " VALUES (%s, %s, NULL, %s, %s, %s, %s)"
            " ON CONFLICT (payload_hash, rule_name) DO NOTHING",
            rows,
        )
        quarantined = cur.rowcount
    if quarantined:
        logger.warning("Quarantined %d record(s) with no id", quarantined)
    return quarantined
