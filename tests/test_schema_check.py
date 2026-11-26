"""Tests for API change detection: the metadata check and the per-page key check."""

from datetime import date

import pytest
import responses
from responses import matchers

from pipeline.ingestion.checkpoint import load_checkpoint
from pipeline.ingestion.client import SocrataClient
from pipeline.ingestion.expected_schema import EXPECTED_SCHEMA
from pipeline.ingestion.ingest import ingest
from pipeline.ingestion.schema_check import (
    COLUMN_ADDED,
    COLUMN_REMOVED,
    SILVER_SOURCE_COLUMNS,
    TYPE_CHANGED,
    ApiSchemaChangeError,
    SchemaChange,
    check_api_schema,
    compare_schema,
)
from pipeline.quality.model import CrimeRecord

BASE_URL = "https://api.test"
URL = f"{BASE_URL}/resource/ijzp-q8t2.json"
METADATA_URL = f"{BASE_URL}/api/views/ijzp-q8t2.json"
COMPUTED = ":@computed_region_awaf_s7ux"
START, END = date(2024, 1, 1), date(2024, 1, 31)


def metadata(schema=None, **changes):
    """Metadata columns for EXPECTED_SCHEMA plus a computed column; changes set a type,
    or with None remove the column."""
    schema = dict(EXPECTED_SCHEMA if schema is None else schema)
    for column, type_ in changes.items():
        if type_ is None:
            schema.pop(column)
        else:
            schema[column] = type_
    columns = [{"fieldName": k, "dataTypeName": v} for k, v in schema.items()]
    columns.append({"fieldName": COMPUTED, "dataTypeName": "number"})
    return {"columns": columns}


@pytest.fixture
def mocked_api():
    # Any request that isn't registered raises, so no test can reach the real API.
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def client():
    return SocrataClient(BASE_URL, page_size=3, sleep=lambda seconds: None)


def saved_changes(db):
    return db.execute(
        "SELECT run_id, endpoint, change_type, column_name, expected_type, actual_type"
        " FROM ops.api_schema_changes ORDER BY column_name, change_type"
    ).fetchall()


# --- compare_schema ----------------------------------------------------------------------


def test_identical_schemas_have_no_changes():
    assert compare_schema(EXPECTED_SCHEMA, dict(EXPECTED_SCHEMA)) == []


def test_every_kind_of_change_is_found_in_column_order():
    expected = {"a": "text", "b": "number", "c": "text"}
    actual = {"a": "text", "b": "text", "d": "checkbox"}

    assert compare_schema(expected, actual) == [
        SchemaChange(TYPE_CHANGED, "b", "number", "text"),
        SchemaChange(COLUMN_REMOVED, "c", "text", None),
        SchemaChange(COLUMN_ADDED, "d", None, "checkbox"),
    ]


def test_computed_region_columns_are_ignored_on_both_sides():
    assert compare_schema({"a": "text", COMPUTED: "number"}, {"a": "text"}) == []
    assert compare_schema({"a": "text"}, {"a": "text", ":@computed_region_new": "number"}) == []


def test_silver_columns_are_the_crime_record_source_columns():
    assert "date" in SILVER_SOURCE_COLUMNS and "occurred_at" not in SILVER_SOURCE_COLUMNS
    assert len(SILVER_SOURCE_COLUMNS) == len(CrimeRecord.model_fields)
    # Every column silver reads must be one we expect from the API...
    assert SILVER_SOURCE_COLUMNS <= EXPECTED_SCHEMA.keys()
    # ...and the only expected column silver doesn't read is the nested location.
    assert EXPECTED_SCHEMA.keys() - SILVER_SOURCE_COLUMNS == {"location"}


# --- check_api_schema --------------------------------------------------------------------


def test_no_change_writes_nothing(db, make_run, mocked_api, client):
    mocked_api.get(METADATA_URL, json=metadata())

    assert check_api_schema(make_run(), conn=db, client=client) == []
    assert saved_changes(db) == []


def test_a_new_column_is_recorded_and_the_run_continues(db, make_run, mocked_api, client):
    run_id = make_run()
    mocked_api.get(METADATA_URL, json=metadata(victim_age="number"))

    changes = check_api_schema(run_id, conn=db, client=client)

    assert changes == [SchemaChange(COLUMN_ADDED, "victim_age", None, "number")]
    assert saved_changes(db) == [
        (run_id, "ijzp-q8t2", COLUMN_ADDED, "victim_age", None, "number"),
    ]


def test_a_removed_silver_column_is_recorded_then_raises(db, make_run, mocked_api, client):
    run_id = make_run()
    mocked_api.get(METADATA_URL, json=metadata(ward=None, block=None))

    with pytest.raises(ApiSchemaChangeError, match="'block'.*removed.*'ward'.*removed") as error:
        check_api_schema(run_id, conn=db, client=client)

    assert [c.column_name for c in error.value.changes] == ["block", "ward"]
    # Recorded before raising, so the failed run shows why.
    assert saved_changes(db) == [
        (run_id, "ijzp-q8t2", COLUMN_REMOVED, "block", "text", None),
        (run_id, "ijzp-q8t2", COLUMN_REMOVED, "ward", "number", None),
    ]


def test_a_retyped_silver_column_raises(db, make_run, mocked_api, client):
    mocked_api.get(METADATA_URL, json=metadata(arrest="text"))

    with pytest.raises(ApiSchemaChangeError, match="'arrest' changed from checkbox to text"):
        check_api_schema(make_run(), conn=db, client=client)
    assert [row[2:] for row in saved_changes(db)] == [
        (TYPE_CHANGED, "arrest", "checkbox", "text"),
    ]


def test_a_removed_column_silver_does_not_use_is_only_recorded(db, make_run, mocked_api, client):
    mocked_api.get(METADATA_URL, json=metadata(location=None))

    changes = check_api_schema(make_run(), conn=db, client=client)

    assert changes == [SchemaChange(COLUMN_REMOVED, "location", "location", None)]
    assert len(saved_changes(db)) == 1


def test_a_new_column_next_to_a_breaking_change_is_recorded_too(db, make_run, mocked_api, client):
    mocked_api.get(METADATA_URL, json=metadata(year=None, year_reported="number"))

    with pytest.raises(ApiSchemaChangeError) as error:
        check_api_schema(make_run(), conn=db, client=client)

    assert [c.column_name for c in error.value.changes] == ["year"]  # only the breaking one
    assert [row[2:4] for row in saved_changes(db)] == [
        (COLUMN_REMOVED, "year"), (COLUMN_ADDED, "year_reported"),
    ]


def test_checking_twice_in_one_run_records_each_change_once(db, make_run, mocked_api, client):
    run_id = make_run()
    mocked_api.get(METADATA_URL, json=metadata(victim_age="number"))

    check_api_schema(run_id, conn=db, client=client)
    check_api_schema(run_id, conn=db, client=client)

    assert len(saved_changes(db)) == 1


# --- during ingest() ---------------------------------------------------------------------


def expect_page(mocked_api, after_id, rows):
    where = "date >= '2024-01-01T00:00:00' AND date < '2024-02-01T00:00:00'"
    if after_id is not None:
        where = f"id > {after_id} AND {where}"
    mocked_api.get(
        URL,
        json=rows,
        match=[matchers.query_param_matcher({"$limit": "3", "$order": "id", "$where": where})],
    )


def test_an_unknown_record_key_is_recorded_once_per_run(db, make_run, mocked_api, client):
    run_id = make_run()
    mocked_api.get(METADATA_URL, json=metadata())
    expect_page(mocked_api, None, [
        {"id": "1", "victim_age": "34"},
        {"id": "2", "victim_age": "40", "flags": ["x"]},
        {"id": "3"},
    ])
    expect_page(mocked_api, 3, [{"id": "4", "victim_age": "21"}])

    ingest(run_id, "backfill", START, END, conn=db, client=client)

    assert saved_changes(db) == [
        (run_id, "ijzp-q8t2", COLUMN_ADDED, "flags", None, "json:array"),
        (run_id, "ijzp-q8t2", COLUMN_ADDED, "victim_age", None, "json:string"),
    ]


def test_missing_keys_are_never_reported(db, make_run, mocked_api, client):
    mocked_api.get(METADATA_URL, json=metadata())
    expect_page(mocked_api, None, [{"id": "1"}, {"id": "2", "arrest": True}])

    stats = ingest(make_run(), "backfill", START, END, conn=db, client=client)

    assert stats.rows_inserted == 2
    assert saved_changes(db) == []


def test_a_breaking_change_stops_ingest_before_any_data_is_fetched(
    db, make_run, mocked_api, client
):
    run_id = make_run()
    # Only the metadata is mocked: a data request would raise ConnectionError instead.
    mocked_api.get(METADATA_URL, json=metadata(updated_on="text"))

    with pytest.raises(ApiSchemaChangeError, match="'updated_on'"):
        ingest(run_id, "backfill", START, END, conn=db, client=client)

    assert db.execute("SELECT count(*) FROM bronze.raw_records").fetchone()[0] == 0
    assert load_checkpoint(db, "backfill:ijzp-q8t2:2024-01-01:2024-01-31") is None
    assert [row[2:4] for row in saved_changes(db)] == [(TYPE_CHANGED, "updated_on")]
