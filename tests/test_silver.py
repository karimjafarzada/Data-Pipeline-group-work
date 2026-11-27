"""Integration tests for build_silver(): newest promotable version per record, upserted."""

from datetime import datetime

import psycopg
import pytest

from pipeline.ingestion.bronze import CHICAGO, write_page
from pipeline.ingestion.client import DATASET_ID
from pipeline.ingestion.ingest import IngestStats
from pipeline.quality.gate import QualityGateError, run_quality_gate
from pipeline.quality.release import RELEASE, release_run
from pipeline.quality.validate import MODEL_MISMATCH, validate
from pipeline.silver.build import SilverBuildError, SilverStats, build_silver

NOW = datetime(2026, 11, 26, 12, 0, tzinfo=CHICAGO)
OLDER = "2026-11-20T08:00:00.000"
BASE = "2026-11-24T15:44:55.000"  # crime_record's updated_on
NEWER = "2026-11-25T09:00:00.000"


@pytest.fixture
def record(crime_record):
    """A valid record with the given id, plus changed fields."""

    def _record(record_id, **changes):
        return {**crime_record, "id": str(record_id), **changes}

    return _record


def load(db, run_id, records, *, gate=True):
    """ingest (straight into bronze) -> validate -> gate, as the CLI runs them."""
    write_page(db, run_id, DATASET_ID, records)
    validation_stats = validate(run_id, conn=db, now=NOW)
    if gate:
        run_quality_gate(
            run_id, IngestStats(rows_fetched=len(records)), validation_stats,
            conn=db, threshold="0.05",
        )


def silver(db):
    """{id: (description, source_updated_at as API text, run_id)} for every silver row."""
    rows = db.execute(
        "SELECT id, description, source_updated_at, run_id FROM silver.crimes ORDER BY id"
    ).fetchall()
    return {
        row[0]: (row[1], row[2].astimezone(CHICAGO).strftime("%Y-%m-%dT%H:%M:%S.000"), row[3])
        for row in rows
    }


def stats_tuple(stats):
    return (stats.candidates, stats.records, stats.inserted, stats.updated, stats.unchanged,
            stats.rejected)


def unhandled(db):
    return db.execute(
        "SELECT count(*) FROM bronze.raw_records WHERE silver_run_id IS NULL"
    ).fetchone()[0]


def test_promotes_typed_rows_with_their_bronze_row_and_run(db, make_run, crime_record):
    run_id = make_run()
    load(db, run_id, [crime_record])
    silver_run = make_run()

    stats = build_silver(silver_run, conn=db)

    row = db.execute(
        "SELECT id, case_number, occurred_at, district, beat, ward, community_area, arrest,"
        " latitude, year, source_updated_at, bronze_id, run_id FROM silver.crimes"
    ).fetchone()
    bronze_id = db.execute("SELECT id FROM bronze.raw_records").fetchone()[0]
    assert row == (
        14391668, "JK491439", datetime(2026, 11, 16, 10, 30, tzinfo=CHICAGO), "008", "0831",
        18, 66, False, 41.760905907, 2026, datetime(2026, 11, 24, 15, 44, 55, tzinfo=CHICAGO),
        bronze_id, silver_run,
    )
    assert stats_tuple(stats) == (1, 1, 1, 0, 0, 0)
    assert stats.seconds >= 0
    marked = db.execute("SELECT silver_run_id FROM bronze.raw_records").fetchone()[0]
    assert marked == silver_run


def test_running_twice_gives_the_same_table(db, make_run, record):
    load(db, make_run(), [record(1), record(2), record(3, latitude=None, longitude=None)])
    build_silver(make_run(), conn=db)
    table_after_first = silver(db)

    second = build_silver(make_run(), conn=db)

    assert silver(db) == table_after_first  # same rows, same run_ids
    assert second == SilverStats(seconds=second.seconds)  # nothing to do


def test_a_newer_version_replaces_the_older_one(db, make_run, record):
    load(db, make_run(), [record(1, description="FIRST")])
    build_silver(make_run(), conn=db)
    load(db, make_run(), [record(1, description="SECOND", updated_on=NEWER)])
    second_build = make_run()

    stats = build_silver(second_build, conn=db)

    assert silver(db) == {1: ("SECOND", NEWER, second_build)}
    assert stats_tuple(stats) == (1, 1, 0, 1, 0, 0)


def test_the_newest_version_wins_within_one_build(db, make_run, record):
    load(db, make_run(), [record(1, description="FIRST")])
    load(db, make_run(), [record(1, description="SECOND", updated_on=NEWER)])

    stats = build_silver(make_run(), conn=db)

    assert silver(db)[1][:2] == ("SECOND", NEWER)
    assert stats_tuple(stats) == (2, 1, 1, 0, 0, 0)
    assert unhandled(db) == 0  # the losing version is marked handled too


def test_on_equal_updated_on_the_later_bronze_row_wins(db, make_run, record):
    load(db, make_run(), [record(1, description="FIRST")])
    load(db, make_run(), [record(1, description="CORRECTED")])  # same updated_on

    build_silver(make_run(), conn=db)

    assert silver(db)[1][0] == "CORRECTED"


def test_an_older_version_never_replaces_a_newer_one(db, make_run, record):
    # The older version is held by a failed run, the newer one is promoted first...
    held = make_run()
    with pytest.raises(QualityGateError):
        load(db, held, [record(1, description="OLD", updated_on=OLDER), record(2, ward="N/A")])
    load(db, make_run(), [record(1, description="NEW", updated_on=NEWER)])
    first_build = make_run()
    build_silver(first_build, conn=db)

    # ...then the older version is released and reaches the next build.
    release_run(held, RELEASE, released_by="amin", note="test", conn=db)
    stats = build_silver(make_run(), conn=db)

    assert silver(db) == {1: ("NEW", NEWER, first_build)}
    assert stats_tuple(stats) == (1, 1, 0, 0, 1, 0)
    assert unhandled(db) == 1  # only the invalid row (ward N/A) stays unhandled


def test_held_runs_are_excluded(db, make_run, record):
    failed = make_run()
    with pytest.raises(QualityGateError):
        load(db, failed, [record(1), record(2, ward="N/A")])
    load(db, make_run(), [record(3)], gate=False)  # crashed before the gate

    stats = build_silver(make_run(), conn=db)

    assert silver(db) == {}
    assert stats.candidates == 0
    assert unhandled(db) == 3


def test_released_runs_are_included_in_the_next_build(db, make_run, record):
    failed = make_run()
    with pytest.raises(QualityGateError):
        load(db, failed, [record(1), record(2, ward="N/A")])
    build_silver(make_run(), conn=db)

    release_run(failed, RELEASE, released_by="amin", note="one bad ward", conn=db)
    stats = build_silver(make_run(), conn=db)

    assert list(silver(db)) == [1]
    assert stats_tuple(stats) == (1, 1, 1, 0, 0, 0)


def force_valid(db, run_id):
    """Mark run_id's bronze rows valid and its gate passed, skipping validate(): as if a
    rule or the model changed after these rows were validated."""
    db.execute(
        "UPDATE bronze.raw_records SET validation_status = 'valid', validated_run_id = %s"
        " WHERE run_id = %s",
        (run_id, run_id),
    )
    db.execute(
        "INSERT INTO ops.quality_gate_results (run_id, checked, failed, threshold, passed,"
        " decided_at) VALUES (%s, 1, 0, 0.05, true, now())",
        (run_id,),
    )


def test_a_record_failing_crime_record_is_reported_and_silver_keeps_the_last_valid_version(
    db, make_run, record
):
    load(db, make_run(), [record(1, description="GOOD")])
    first_build = make_run()
    build_silver(first_build, conn=db)
    drifted = make_run()
    # iucr as a number passes every rule but not CrimeRecord (silver needs text).
    write_page(db, drifted, DATASET_ID, [record(1, iucr=1320, updated_on=NEWER), record(2)])
    force_valid(db, drifted)
    second_build = make_run()

    stats = build_silver(second_build, conn=db)

    assert stats_tuple(stats) == (2, 2, 1, 0, 0, 1)
    assert silver(db) == {1: ("GOOD", BASE, first_build), 2: ("TO VEHICLE", BASE, second_build)}
    quarantined = db.execute(
        "SELECT run_id, source_id, rule_name, reason FROM ops.quarantine"
    ).fetchall()
    assert quarantined == [
        (second_build, "1", MODEL_MISMATCH, "iucr: Input should be a valid string"),
    ]
    status = db.execute(
        "SELECT validation_status, silver_run_id FROM bronze.raw_records"
        " WHERE payload->>'updated_on' = %s",
        (NEWER,),
    ).fetchone()
    assert status == ("invalid", None)


def test_a_rejected_newest_version_falls_back_to_an_older_candidate(db, make_run, record):
    load(db, make_run(), [record(1, description="OLDER BUT VALID")])
    drifted = make_run()
    write_page(db, drifted, DATASET_ID, [record(1, iucr=1320, updated_on=NEWER)])
    force_valid(db, drifted)

    stats = build_silver(make_run(), conn=db)

    assert silver(db)[1][:2] == ("OLDER BUT VALID", BASE)
    assert stats_tuple(stats) == (2, 1, 1, 0, 0, 1)


def test_each_row_keeps_the_run_that_last_wrote_it(db, make_run, record):
    load(db, make_run(), [record(1), record(2)])
    first_build = make_run()
    build_silver(first_build, conn=db)
    load(db, make_run(), [record(2, description="CHANGED", updated_on=NEWER)])
    second_build = make_run()

    build_silver(second_build, conn=db)

    assert {k: v[2] for k, v in silver(db).items()} == {1: first_build, 2: second_build}


def test_works_across_several_batches(db, make_run, record):
    load(db, make_run(), [record(n) for n in range(1, 8)])

    stats = build_silver(make_run(), conn=db, batch_size=2)

    assert list(silver(db)) == list(range(1, 8))
    assert stats_tuple(stats) == (7, 7, 7, 0, 0, 0)


def test_empty_bronze_builds_nothing(db, make_run):
    assert build_silver(make_run(), conn=db).candidates == 0


def test_another_build_running_raises_and_changes_nothing(db, make_run, record, pg_server):
    load(db, make_run(), [record(1)])
    with psycopg.connect(**{**pg_server, "dbname": db.info.dbname}, autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(hashtext('build_silver'))")

        with pytest.raises(SilverBuildError, match="already running"):
            build_silver(make_run(), conn=db)

    assert silver(db) == {}
    assert unhandled(db) == 1


def test_autocommit_setting_is_restored(db, make_run):
    run_id = make_run()
    db.autocommit = False
    build_silver(run_id, conn=db)
    assert db.autocommit is False


def test_batch_size_must_be_positive(db, make_run):
    with pytest.raises(ValueError):
        build_silver(make_run(), conn=db, batch_size=0)
