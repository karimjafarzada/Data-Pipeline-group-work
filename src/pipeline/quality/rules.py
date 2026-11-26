"""Validation rules for crime records (see CLAUDE.md, "Validation rules").

Each rule looks at one raw API payload and returns a Violation, or None when the
record passes. A rule reports at most one Violation per record, because quarantine
keeps one row per (payload_hash, rule_name); several problems for the same rule are
joined into one reason.

The API leaves out null fields, so a missing key is only a problem for the required
fields. The other rules skip values that are missing, and the cross-field rules skip
values that can't be read, so one bad value is reported by one rule only.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pipeline.ingestion.bronze import Record, parse_source_timestamp

MISSING_REQUIRED_FIELD = "missing_required_field"
INVALID_TIMESTAMP = "invalid_timestamp"
INVALID_NUMBER = "invalid_number"
INVALID_BOOLEAN = "invalid_boolean"
YEAR_MISMATCH = "year_mismatch"
COORDINATES_INCOMPLETE = "coordinates_incomplete"
COORDINATES_OUT_OF_RANGE = "coordinates_out_of_range"
UPDATED_BEFORE_DATE = "updated_before_date"

# Silver declares these NOT NULL, so a record without them can't be loaded.
REQUIRED_FIELDS = (
    "case_number",
    "date",
    "iucr",
    "primary_type",
    "arrest",
    "domestic",
    "year",
    "updated_on",
)
TIMESTAMP_FIELDS = ("date", "updated_on")
INTEGER_FIELDS = ("id", "ward", "community_area", "year")
COORDINATE_FIELDS = ("x_coordinate", "y_coordinate", "latitude", "longitude")
BOOLEAN_FIELDS = ("arrest", "domestic")

# Rough box around Chicago (CLAUDE.md).
LATITUDE_RANGE = (41.6, 42.1)
LONGITUDE_RANGE = (-88.0, -87.5)

_INTEGER = re.compile(r"-?\d+")
_DECIMAL = re.compile(r"-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?")


@dataclass(frozen=True)
class Violation:
    rule_name: str
    reason: str


# --- helpers ----------------------------------------------------------------


def is_missing(value: Any) -> bool:
    """None or a blank string. Both mean 'no value'."""
    return value is None or (isinstance(value, str) and value.strip() == "")


def parse_int(value: Any) -> int | None:
    """An int, or a string of digits like '18' or '-3'. Anything else gives None."""
    if isinstance(value, bool):  # bool is a subclass of int in Python
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and _INTEGER.fullmatch(value):
        return int(value)
    return None


def parse_float(value: Any) -> float | None:
    """A finite number, or a string like '41.76' or '1159193'. Anything else gives None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) or (isinstance(value, str) and _DECIMAL.fullmatch(value)):
        number = float(value)
    else:
        return None
    # Reject NaN and infinity ('1e999' turns into infinity).
    return number if math.isfinite(number) else None


def _timestamp(record: Record, field: str) -> datetime | None:
    return parse_source_timestamp(record.get(field))


def _violation(rule_name: str, problems: list[str]) -> Violation | None:
    return Violation(rule_name, "; ".join(problems)) if problems else None


# --- rules ------------------------------------------------------------------


def check_required_fields(record: Record) -> Violation | None:
    missing = [field for field in REQUIRED_FIELDS if is_missing(record.get(field))]
    if not missing:
        return None
    return Violation(MISSING_REQUIRED_FIELD, f"missing required field(s): {', '.join(missing)}")


def check_timestamps(record: Record, now: datetime) -> Violation | None:
    problems = []
    for field in TIMESTAMP_FIELDS:
        value = record.get(field)
        if not is_missing(value) and _timestamp(record, field) is None:
            problems.append(f"{field} {value!r} is not a valid timestamp")
    occurred_at = _timestamp(record, "date")
    if occurred_at is not None and occurred_at > now:
        problems.append(f"date {record['date']!r} is in the future")
    return _violation(INVALID_TIMESTAMP, problems)


def check_numbers(record: Record) -> Violation | None:
    problems = []
    for field in INTEGER_FIELDS:
        value = record.get(field)
        if not is_missing(value) and parse_int(value) is None:
            problems.append(f"{field} {value!r} is not an integer")
    for field in COORDINATE_FIELDS:
        value = record.get(field)
        if not is_missing(value) and parse_float(value) is None:
            problems.append(f"{field} {value!r} is not a finite number")
    return _violation(INVALID_NUMBER, problems)


def check_booleans(record: Record) -> Violation | None:
    problems = []
    for field in BOOLEAN_FIELDS:
        value = record.get(field)
        if not is_missing(value) and not isinstance(value, bool):
            problems.append(f"{field} {value!r} is not true or false")
    return _violation(INVALID_BOOLEAN, problems)


def check_year_matches_date(record: Record) -> Violation | None:
    year = parse_int(record.get("year"))
    occurred_at = _timestamp(record, "date")
    if year is None or occurred_at is None or year == occurred_at.year:
        return None
    return Violation(YEAR_MISMATCH, f"year {year} but date {record['date']!r} is in {occurred_at.year}")


def check_coordinates_complete(record: Record) -> Violation | None:
    has_latitude = not is_missing(record.get("latitude"))
    has_longitude = not is_missing(record.get("longitude"))
    if has_latitude == has_longitude:  # both there, or both missing
        return None
    present, absent = ("latitude", "longitude") if has_latitude else ("longitude", "latitude")
    return Violation(COORDINATES_INCOMPLETE, f"{present} present but {absent} missing")


def check_coordinates_in_range(record: Record) -> Violation | None:
    problems = []
    for field, (low, high) in (("latitude", LATITUDE_RANGE), ("longitude", LONGITUDE_RANGE)):
        value = parse_float(record.get(field))
        if value is not None and not low <= value <= high:
            problems.append(f"{field} {value} outside {low} to {high}")
    return _violation(COORDINATES_OUT_OF_RANGE, problems)


def check_updated_after_date(record: Record) -> Violation | None:
    occurred_at = _timestamp(record, "date")
    updated_at = _timestamp(record, "updated_on")
    if occurred_at is None or updated_at is None or updated_at >= occurred_at:
        return None
    return Violation(
        UPDATED_BEFORE_DATE,
        f"updated_on {record['updated_on']!r} is before date {record['date']!r}",
    )


def check_record(record: Record, now: datetime) -> list[Violation]:
    """Run every rule. Returns one Violation per broken rule; empty means valid."""
    results = [
        check_required_fields(record),
        check_timestamps(record, now),
        check_numbers(record),
        check_booleans(record),
        check_year_matches_date(record),
        check_coordinates_complete(record),
        check_coordinates_in_range(record),
        check_updated_after_date(record),
    ]
    return [violation for violation in results if violation is not None]
