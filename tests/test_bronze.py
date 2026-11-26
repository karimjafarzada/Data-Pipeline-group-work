"""Tests for writing pages to bronze."""

import hashlib
from datetime import UTC, datetime

import pytest

from pipeline.ingestion.bronze import parse_source_timestamp, payload_hash, write_page

ENDPOINT = "ijzp-q8t2"


def record(id_, **fields):
    return {"id": str(id_), "updated_on": "2024-01-31T14:00:00.000", **fields}


def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# --- payload hash (no database) ---------------------------------------------


def test_payload_hash_uses_sorted_keys_and_no_spaces():
    assert payload_hash({"b": 1, "a": "x"}) == sha256('{"a":"x","b":1}')


def test_payload_hash_ignores_key_order():
    assert payload_hash({"id": "1", "arrest": False}) == payload_hash({"arrest": False, "id": "1"})


def test_payload_hash_changes_when_a_value_changes():
    assert payload_hash({"id": "1", "arrest": False}) != payload_hash({"id": "1", "arrest": True})


def test_payload_hash_keeps_non_ascii_characters():
    assert payload_hash({"block": "Ñ"}) == sha256('{"block":"Ñ"}')


# --- source timestamps (no database) ----------------------------------------


@pytest.mark.parametrize(
    ("api_value", "utc"),
    [
        ("2024-01-31T14:00:00.000", datetime(2024, 1, 31, 20, 0, tzinfo=UTC)),  # winter: UTC-6
        ("2024-07-01T12:00:00.000", datetime(2024, 7, 1, 17, 0, tzinfo=UTC)),  # summer: UTC-5
    ],
)
def test_source_timestamps_are_read_as_chicago_time(api_value, utc):
    assert parse_source_timestamp(api_value) == utc


@pytest.mark.parametrize("bad_value", [None, "", "not a date", 12345])
def test_missing_or_bad_timestamps_become_none(bad_value):
    assert parse_source_timestamp(bad_value) is None


# --- writing to the database ------------------------------------------------


def bronze_rows(db):
    return db.execute(
        "SELECT source_id, payload, payload_hash, source_updated_at, run_id"
        " FROM bronze.raw_records ORDER BY id"
    ).fetchall()


def test_write_page_stores_each_record(db, make_run):
    run_id = make_run()
    page = [record(1, primary_type="THEFT"), record(2)]

    result = write_page(db, run_id, ENDPOINT, page)

    assert (result.inserted, result.duplicates, result.quarantined) == (2, 0, 0)
    first = bronze_rows(db)[0]
    assert first == (
        "1",
        page[0],
        payload_hash(page[0]),
        datetime(2024, 1, 31, 20, 0, tzinfo=UTC),
        run_id,
    )


def test_writing_the_same_page_again_adds_nothing(db, make_run):
    page = [record(1), record(2)]
    write_page(db, make_run(), ENDPOINT, page)

    result = write_page(db, make_run(), ENDPOINT, page)

    assert (result.inserted, result.duplicates) == (0, 2)
    assert len(bronze_rows(db)) == 2


def test_a_changed_record_gets_a_new_row(db, make_run):
    write_page(db, make_run(), ENDPOINT, [record(1, arrest=False)])

    result = write_page(
        db, make_run(), ENDPOINT, [record(1, arrest=True, updated_on="2024-02-05T09:00:00.000")]
    )

    assert result.inserted == 1
    assert [row[0] for row in bronze_rows(db)] == ["1", "1"]


def test_records_without_id_go_to_quarantine_once(db, make_run):
    no_id = {"case_number": "JA1", "updated_on": "2024-01-31T14:00:00.000"}
    first = write_page(db, make_run(), ENDPOINT, [record(1), no_id])
    second = write_page(db, make_run(), ENDPOINT, [no_id])

    quarantine = db.execute(
        "SELECT source_id, rule_name, payload FROM ops.quarantine"
    ).fetchall()
    assert (first.inserted, first.quarantined) == (1, 1)
    assert second.quarantined == 0
    assert quarantine == [(None, "missing_source_id", no_id)]
    assert [row[0] for row in bronze_rows(db)] == ["1"]
