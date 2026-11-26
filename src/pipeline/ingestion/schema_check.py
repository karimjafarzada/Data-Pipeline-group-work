"""API change detection (CLAUDE.md, "API change detection").

Two checks, both writing to ops.api_schema_changes:

- check_api_schema(run_id): at the start of ingest(), before any data is fetched. It
  compares the dataset's metadata (fieldName / dataTypeName) with EXPECTED_SCHEMA.
  A new column is only recorded. A removed or retyped column that silver uses raises
  ApiSchemaChangeError, so the run fails before fetching anything.
- record_unexpected_keys(): for every page. A record key that isn't in EXPECTED_SCHEMA
  is recorded as 'column_added', once per run. Missing keys are normal (the API leaves
  out nulls) and are never reported.

':@computed_region_*' columns are ignored everywhere: the metadata lists them, but
records never contain them.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import psycopg

from pipeline.ingestion.client import DATASET_ID, Record, SocrataClient
from pipeline.ingestion.expected_schema import EXPECTED_SCHEMA
from pipeline.quality.model import CrimeRecord

logger = logging.getLogger(__name__)

COLUMN_ADDED = "column_added"
COLUMN_REMOVED = "column_removed"
TYPE_CHANGED = "type_changed"
COMPUTED_PREFIX = ":@computed_region_"

# The API columns silver reads: each CrimeRecord field's alias (e.g. 'date' for
# occurred_at), or its own name. Losing or retyping one of these breaks silver.
SILVER_SOURCE_COLUMNS = frozenset(
    field.validation_alias if isinstance(field.validation_alias, str) else name
    for name, field in CrimeRecord.model_fields.items()
)


@dataclass(frozen=True)
class SchemaChange:
    change_type: str  # column_added, column_removed or type_changed
    column_name: str
    expected_type: str | None  # None for an added column
    actual_type: str | None  # None for a removed column

    def describe(self) -> str:
        if self.change_type == COLUMN_ADDED:
            return f"new column {self.column_name!r} ({self.actual_type})"
        if self.change_type == COLUMN_REMOVED:
            return f"column {self.column_name!r} ({self.expected_type}) was removed"
        return (
            f"column {self.column_name!r} changed from {self.expected_type}"
            f" to {self.actual_type}"
        )


class ApiSchemaChangeError(Exception):
    """A column silver uses was removed or changed type; the run must not fetch data."""

    def __init__(self, changes: list[SchemaChange]) -> None:
        self.changes = changes
        super().__init__(
            "The API changed columns that silver uses: "
            + "; ".join(change.describe() for change in changes)
            + ". Update expected_schema.py and the code that reads these columns."
        )


def compare_schema(expected: Mapping[str, str], actual: Mapping[str, str]) -> list[SchemaChange]:
    """Every difference between two fieldName -> dataTypeName maps, sorted by column."""
    expected = {k: v for k, v in expected.items() if not _is_computed(k)}
    actual = {k: v for k, v in actual.items() if not _is_computed(k)}
    changes = []
    for column in sorted(expected.keys() | actual.keys()):
        if column not in actual:
            changes.append(SchemaChange(COLUMN_REMOVED, column, expected[column], None))
        elif column not in expected:
            changes.append(SchemaChange(COLUMN_ADDED, column, None, actual[column]))
        elif expected[column] != actual[column]:
            changes.append(SchemaChange(TYPE_CHANGED, column, expected[column], actual[column]))
    return changes


def check_api_schema(
    run_id: UUID,
    *,
    conn: psycopg.Connection | None = None,
    client: SocrataClient | None = None,
) -> list[SchemaChange]:
    """Compare the API's columns with EXPECTED_SCHEMA and record every difference.

    The differences are committed BEFORE ApiSchemaChangeError is raised, so a failed
    run still shows why. ``conn`` and ``client`` are for tests; by default they come
    from the environment.
    """
    if client is None:
        client = SocrataClient.from_env()
    actual = {column["fieldName"]: column["dataTypeName"] for column in client.fetch_columns()}
    changes = compare_schema(EXPECTED_SCHEMA, actual)

    if changes:
        owns_conn = conn is None
        if conn is None:
            from pipeline.db import get_connection  # Student 3's module, not on main yet

            conn = get_connection()
        try:
            with conn.transaction():
                _save(conn, run_id, changes)
        finally:
            if owns_conn:
                conn.close()
        for change in changes:
            logger.warning("API schema change: %s", change.describe())

    breaking = [
        change for change in changes
        if change.change_type != COLUMN_ADDED and change.column_name in SILVER_SOURCE_COLUMNS
    ]
    if breaking:
        raise ApiSchemaChangeError(breaking)
    logger.info("API schema check: %d change(s), none that silver depends on", len(changes))
    return changes


def record_unexpected_keys(
    conn: psycopg.Connection, run_id: UUID, page: Iterable[Record], seen: set[str]
) -> int:
    """Record keys of this page that aren't in EXPECTED_SCHEMA as 'column_added'.

    ``seen`` holds the keys already recorded in this run and is updated, so each key
    is recorded once per run. Call it inside the page's transaction. Returns how many
    new keys were recorded.
    """
    new: dict[str, str] = {}
    for record in page:
        for key, value in record.items():
            if key in EXPECTED_SCHEMA or key in seen or key in new or _is_computed(key):
                continue
            new[key] = f"json:{_json_type(value)}"
    if not new:
        return 0
    changes = [SchemaChange(COLUMN_ADDED, key, None, json_type) for key, json_type in new.items()]
    _save(conn, run_id, changes)
    seen.update(new)
    for change in changes:
        logger.warning("Record key not in the expected schema: %s", change.describe())
    return len(new)


def _save(conn: psycopg.Connection, run_id: UUID, changes: list[SchemaChange]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO ops.api_schema_changes"
            " (run_id, endpoint, change_type, column_name, expected_type, actual_type)"
            " VALUES (%s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (run_id, endpoint, change_type, column_name) DO NOTHING",
            [(run_id, DATASET_ID, c.change_type, c.column_name, c.expected_type, c.actual_type)
             for c in changes],
        )


def _is_computed(column: str) -> bool:
    return column.startswith(COMPUTED_PREFIX)


def _json_type(value: Any) -> str:
    if isinstance(value, bool):  # before int: True is an int in Python
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "null"
