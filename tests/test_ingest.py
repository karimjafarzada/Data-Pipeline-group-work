"""Tests for resumable ingestion: real PostgreSQL, mocked API."""

from datetime import date

import psycopg
import pytest
import requests
import responses
from responses import matchers

from pipeline.ingestion.checkpoint import load_checkpoint
from pipeline.ingestion.client import SocrataClient
from pipeline.ingestion.ingest import IngestionError, ingest

BASE_URL = "https://api.test"
URL = f"{BASE_URL}/resource/ijzp-q8t2.json"
START, END = date(2024, 1, 1), date(2024, 1, 31)
DATES = "date >= '2024-01-01T00:00:00' AND date < '2024-02-01T00:00:00'"
T1 = "2024-01-31T14:00:00.000"
T2 = "2024-02-05T09:30:00.000"


@pytest.fixture
def mocked_api():
    # Any request that isn't registered raises, so no test can reach the real API.
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def client():
    # Page size 3 keeps the pages small; the fake sleep makes retries instant.
    return SocrataClient(BASE_URL, page_size=3, sleep=lambda seconds: None)


def records(*ids, updated_on=T1):
    return [{"id": str(i), "updated_on": updated_on} for i in ids]


def expect_backfill_page(mocked_api, after_id, **response):
    where = DATES if after_id is None else f"id > {after_id} AND {DATES}"
    mocked_api.get(
        URL,
        match=[matchers.query_param_matcher({"$limit": "3", "$order": "id", "$where": where})],
        **response,
    )


def expect_changes_page(mocked_api, ts, after_id, rows):
    where = f"updated_on > '{ts}' OR (updated_on = '{ts}' AND id > {after_id})"
    mocked_api.get(
        URL,
        json=rows,
        match=[
            matchers.query_param_matcher(
                {"$limit": "3", "$order": "updated_on,id", "$where": where}
            )
        ],
    )


def bronze_ids(db):
    """Bronze source ids, sorted numerically, one entry per row."""
    rows = db.execute("SELECT source_id FROM bronze.raw_records").fetchall()
    return sorted(int(row[0]) for row in rows)


BACKFILL_KEY = "backfill:ijzp-q8t2:2024-01-01:2024-01-31"


# --- backfill: crash and resume ----------------------------------------------


def test_crash_after_page_2_resumes_from_checkpoint_without_duplicates(
    db, make_run, mocked_api, client
):
    # Run 1: pages 1 and 2 are saved, then the network dies while fetching page 3.
    expect_backfill_page(mocked_api, None, json=records(1, 2, 3))
    expect_backfill_page(mocked_api, 3, json=records(4, 5, 6))
    expect_backfill_page(mocked_api, 6, body=requests.ConnectionError("network down"))
    first_run = make_run()

    with pytest.raises(requests.ConnectionError):
        ingest(first_run, "backfill", START, END, conn=db, client=client)

    assert bronze_ids(db) == [1, 2, 3, 4, 5, 6]
    assert load_checkpoint(db, BACKFILL_KEY) == "6"

    # Run 2: must start after id 6 (any other request would not match and fail).
    mocked_api.reset()
    expect_backfill_page(mocked_api, 6, json=records(7, 8, 9))
    expect_backfill_page(mocked_api, 9, json=records(10))
    second_run = make_run()

    stats = ingest(second_run, "backfill", START, END, conn=db, client=client)

    assert (stats.pages, stats.rows_inserted, stats.rows_duplicate) == (2, 4, 0)
    assert bronze_ids(db) == list(range(1, 11))  # every record exactly once
    assert load_checkpoint(db, BACKFILL_KEY) == "10"
    runs = db.execute(
        "SELECT run_id, count(*) FROM bronze.raw_records GROUP BY run_id"
    ).fetchall()
    assert dict(runs) == {first_run: 6, second_run: 4}


def test_crash_inside_a_page_rolls_back_that_page_and_its_checkpoint(
    db, make_run, mocked_api, client, monkeypatch
):
    import pipeline.ingestion.ingest as ingest_module

    real_save = ingest_module.save_checkpoint
    calls = []

    def save_that_fails_on_page_2(*args):
        calls.append(args)
        if len(calls) == 2:  # page 2's rows are already inserted at this point
            raise RuntimeError("crash between bronze insert and checkpoint save")
        real_save(*args)

    monkeypatch.setattr(ingest_module, "save_checkpoint", save_that_fails_on_page_2)
    expect_backfill_page(mocked_api, None, json=records(1, 2, 3))
    expect_backfill_page(mocked_api, 3, json=records(4, 5, 6))

    with pytest.raises(RuntimeError, match="crash between"):
        ingest(make_run(), "backfill", START, END, conn=db, client=client)

    assert bronze_ids(db) == [1, 2, 3]  # page 2's rows were rolled back with its checkpoint
    assert load_checkpoint(db, BACKFILL_KEY) == "3"


# --- backfill: running it twice ----------------------------------------------


def test_rerunning_a_finished_backfill_adds_nothing(db, make_run, mocked_api, client):
    expect_backfill_page(mocked_api, None, json=records(1, 2, 3))
    expect_backfill_page(mocked_api, 3, json=records(4, 5))
    ingest(make_run(), "backfill", START, END, conn=db, client=client)

    mocked_api.reset()
    expect_backfill_page(mocked_api, 5, json=[])
    stats = ingest(make_run(), "backfill", START, END, conn=db, client=client)

    assert (stats.pages, stats.rows_fetched) == (0, 0)
    assert bronze_ids(db) == [1, 2, 3, 4, 5]


def test_same_backfill_from_scratch_twice_gives_the_same_row_count(
    db, make_run, mocked_api, client
):
    def run_full_backfill():
        mocked_api.reset()
        expect_backfill_page(mocked_api, None, json=records(1, 2, 3))
        expect_backfill_page(mocked_api, 3, json=records(4, 5))
        return ingest(make_run(), "backfill", START, END, conn=db, client=client)

    run_full_backfill()
    count_after_first = len(bronze_ids(db))
    db.execute("DELETE FROM ops.ingestion_checkpoint")  # forget where we stopped

    stats = run_full_backfill()

    assert (stats.rows_fetched, stats.rows_inserted, stats.rows_duplicate) == (5, 0, 5)
    assert len(bronze_ids(db)) == count_after_first == 5


# --- incremental ------------------------------------------------------------


def test_incremental_picks_up_a_record_changed_after_first_load(
    db, make_run, mocked_api, client
):
    # Backfill loads records 1 and 2, both last changed at T1.
    expect_backfill_page(mocked_api, None, json=records(1, 2))
    ingest(make_run(), "backfill", START, END, conn=db, client=client)

    # First incremental run has no checkpoint, so it starts at T1 (newest in bronze).
    # Record 1 was changed at T2 (e.g. an arrest was made).
    mocked_api.reset()
    changed = {"id": "1", "updated_on": T2, "arrest": True}
    expect_changes_page(mocked_api, T1, 0, records(1, 2) + [changed])
    expect_changes_page(mocked_api, T2, 1, [])

    stats = ingest(make_run("incremental"), "incremental", conn=db, client=client)

    assert (stats.rows_inserted, stats.rows_duplicate) == (1, 2)
    assert bronze_ids(db) == [1, 1, 2]  # old and new version of record 1
    assert load_checkpoint(db, "incremental:ijzp-q8t2") == f"{T2}|1"

    # The next incremental run continues from the checkpoint.
    mocked_api.reset()
    expect_changes_page(mocked_api, T2, 1, [])
    stats = ingest(make_run("incremental"), "incremental", conn=db, client=client)
    assert stats.pages == 0


def test_incremental_with_empty_bronze_asks_for_a_backfill(db, make_run, client):
    with pytest.raises(IngestionError, match="run a backfill first"):
        ingest(make_run("incremental"), "incremental", conn=db, client=client)


# --- the caller's connection ------------------------------------------------


def test_autocommit_setting_is_restored_after_a_successful_run(
    db, make_run, mocked_api, client
):
    run_id = make_run()
    db.autocommit = False
    expect_backfill_page(mocked_api, None, json=records(1, 2))

    ingest(run_id, "backfill", START, END, conn=db, client=client)

    assert db.autocommit is False
    assert bronze_ids(db) == [1, 2]  # the pages were still committed


def test_autocommit_setting_is_restored_when_ingest_raises(db, make_run, client):
    run_id = make_run("incremental")
    db.autocommit = False

    with pytest.raises(IngestionError):
        ingest(run_id, "incremental", conn=db, client=client)

    assert db.autocommit is False


# --- safety -----------------------------------------------------------------


def test_two_runs_cannot_ingest_the_same_key_at_once(db, make_run, client, pg_server):
    settings = {**pg_server, "dbname": db.info.dbname}
    with psycopg.connect(**settings, autocommit=True) as other_run:
        other_run.execute("SELECT pg_advisory_lock(hashtext(%s))", (BACKFILL_KEY,))

        with pytest.raises(IngestionError, match="already ingesting"):
            ingest(make_run(), "backfill", START, END, conn=db, client=client)


@pytest.mark.parametrize(
    ("mode", "start", "end", "message"),
    [
        ("backfill", None, None, "needs both"),
        ("backfill", END, START, "is after"),
        ("incremental", START, END, "don't take"),
    ],
)
def test_bad_arguments_fail_before_touching_the_database(mode, start, end, message):
    with pytest.raises(ValueError, match=message):
        ingest("not-used", mode, start, end, conn=None, client=None)
