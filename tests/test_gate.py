"""Tests for the quality gate: the rate, the threshold, and the saved decision."""

from datetime import datetime
from decimal import Decimal
from fractions import Fraction

import psycopg
import pytest

from pipeline.ingestion.bronze import CHICAGO, write_page
from pipeline.ingestion.client import DATASET_ID
from pipeline.ingestion.ingest import IngestStats
from pipeline.quality import rules
from pipeline.quality.gate import (
    DEFAULT_THRESHOLD,
    THRESHOLD_ENV,
    QualityGateError,
    check_quality_gate,
    load_threshold,
    parse_threshold,
    run_quality_gate,
)
from pipeline.quality.validate import ValidationStats, validate

NOW = datetime(2026, 11, 26, 12, 0, tzinfo=CHICAGO)


def stats(checked, invalid, no_id=0, rule_hits=None):
    """IngestStats and ValidationStats for `checked` bronze rows, `invalid` of them broken,
    plus `no_id` records quarantined at ingestion."""
    ingest = IngestStats(rows_fetched=checked + no_id, rows_quarantined=no_id)
    validation = ValidationStats(
        checked=checked, valid=checked - invalid, invalid=invalid, rule_hits=rule_hits or {}
    )
    return ingest, validation


# --- check_quality_gate: the rate and the threshold --------------------------------------


def test_below_the_threshold_passes():
    result = check_quality_gate(*stats(100, 4), "0.05")
    assert (result.checked, result.failed, result.rate, result.passed) == (
        100, 4, Fraction(4, 100), True,
    )


@pytest.mark.parametrize("checked, invalid", [(100, 5), (20, 1), (60, 3)])
def test_exactly_at_the_threshold_passes(checked, invalid):
    # "Exceeds" means strictly greater, so a rate equal to the threshold passes.
    assert check_quality_gate(*stats(checked, invalid), "0.05").passed


def test_a_float_threshold_is_compared_exactly():
    assert check_quality_gate(*stats(20, 1), 0.05).passed


def test_above_the_threshold_raises_with_the_numbers():
    with pytest.raises(QualityGateError) as error:
        check_quality_gate(*stats(100, 6), "0.05")

    message = str(error.value)
    assert "6 of 100 records failed (6.00%)" in message
    assert f"above {THRESHOLD_ENV}=0.05 (5.00%)" in message
    assert error.value.result.rate == Fraction(6, 100)
    assert error.value.run_id is None


def test_message_lists_the_broken_rules_most_frequent_first():
    hits = {rules.YEAR_MISMATCH: 1, rules.INVALID_NUMBER: 3, rules.INVALID_BOOLEAN: 1}
    with pytest.raises(QualityGateError, match=(
        r"Broken rules: invalid_number=3, invalid_boolean=1, year_mismatch=1\.$"
    )):
        check_quality_gate(*stats(10, 4, rule_hits=hits), "0.05")


def test_nothing_checked_passes():
    result = check_quality_gate(*stats(0, 0), "0.05")
    assert (result.checked, result.rate, result.passed) == (0, 0, True)


def test_only_records_without_an_id_fails():
    # Nothing reached bronze, but every record the API sent was broken: 2 of 2.
    with pytest.raises(QualityGateError, match=r"2 of 2 records failed \(100\.00%\)") as error:
        check_quality_gate(*stats(0, 0, no_id=2), "0.05")
    assert error.value.result.rule_hits == {"missing_source_id": 2}


def test_records_without_an_id_count_on_both_sides():
    # (1 invalid + 1 without id) / (19 checked + 1 without id) = 2 / 20 = 10%.
    with pytest.raises(QualityGateError) as error:
        check_quality_gate(*stats(19, 1, no_id=1), "0.05")
    assert (error.value.result.failed, error.value.result.checked) == (2, 20)


def test_threshold_zero_fails_on_any_failure():
    assert check_quality_gate(*stats(1000, 0), "0").passed
    with pytest.raises(QualityGateError):
        check_quality_gate(*stats(1000, 1), "0")


def test_threshold_one_never_fails():
    assert check_quality_gate(*stats(10, 10), "1").passed


# --- the threshold setting ---------------------------------------------------------------


def test_threshold_defaults_to_five_percent(monkeypatch):
    monkeypatch.delenv(THRESHOLD_ENV, raising=False)
    assert load_threshold() == DEFAULT_THRESHOLD == Decimal("0.05")


def test_blank_threshold_means_the_default(monkeypatch):
    monkeypatch.setenv(THRESHOLD_ENV, "  ")
    assert load_threshold() == Decimal("0.05")


def test_threshold_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv(THRESHOLD_ENV, "0.1")
    assert load_threshold() == Decimal("0.1")


@pytest.mark.parametrize("bad", ["abc", "-0.1", "1.5", "nan", "inf", "5%"])
def test_invalid_threshold_in_the_environment_raises(monkeypatch, bad):
    monkeypatch.setenv(THRESHOLD_ENV, bad)
    with pytest.raises(ValueError, match=THRESHOLD_ENV):
        load_threshold()


@pytest.mark.parametrize("bad", [-0.01, 1.01, float("nan"), "x"])
def test_invalid_threshold_argument_raises(bad):
    with pytest.raises(ValueError, match="from 0 to 1"):
        parse_threshold(bad)
    with pytest.raises(ValueError):
        check_quality_gate(*stats(10, 0), bad)


# --- run_quality_gate: the saved decision ------------------------------------------------


def decision(db, run_id):
    return db.execute(
        "SELECT checked, failed, failure_rate, threshold, passed, decided_at IS NOT NULL"
        " FROM ops.quality_gate_results WHERE run_id = %s",
        (run_id,),
    ).fetchone()


def test_a_passing_run_saves_its_decision(db, make_run):
    run_id = make_run()

    result = run_quality_gate(run_id, *stats(20, 1), conn=db, threshold="0.05")

    assert result.passed
    assert decision(db, run_id) == (20, 1, Decimal("0.05"), Decimal("0.05"), True, True)


def test_a_failing_run_saves_its_decision_then_raises(db, make_run):
    run_id = make_run()

    with pytest.raises(QualityGateError, match=f"Quality gate failed for run {run_id}") as error:
        run_quality_gate(run_id, *stats(20, 7), conn=db, threshold="0.05")

    assert error.value.run_id == run_id
    assert decision(db, run_id) == (20, 7, Decimal("0.35"), Decimal("0.05"), False, True)


def test_threshold_comes_from_the_environment_by_default(db, make_run, monkeypatch):
    monkeypatch.setenv(THRESHOLD_ENV, "0.5")
    run_id = make_run()

    assert run_quality_gate(run_id, *stats(20, 7), conn=db).passed
    assert decision(db, run_id)[3] == Decimal("0.5")


def test_an_invalid_threshold_saves_nothing(db, make_run, monkeypatch):
    monkeypatch.setenv(THRESHOLD_ENV, "lots")
    run_id = make_run()

    with pytest.raises(ValueError):
        run_quality_gate(run_id, *stats(20, 0), conn=db)
    assert decision(db, run_id) is None


def test_running_the_gate_again_replaces_the_decision(db, make_run):
    run_id = make_run()
    with pytest.raises(QualityGateError):
        run_quality_gate(run_id, *stats(20, 7), conn=db, threshold="0.05")

    run_quality_gate(run_id, *stats(20, 7), conn=db, threshold="0.5")

    count = db.execute("SELECT count(*) FROM ops.quality_gate_results").fetchone()[0]
    assert count == 1
    assert decision(db, run_id)[3:5] == (Decimal("0.5"), True)


def test_the_decision_is_committed_without_autocommit(db, make_run):
    run_id = make_run()
    db.autocommit = False

    run_quality_gate(run_id, *stats(20, 0), conn=db, threshold="0.05")

    db.rollback()  # nothing left to roll back: the decision was already committed
    assert decision(db, run_id)[4] is True


def test_the_decision_needs_an_existing_run(db):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        run_quality_gate(
            "00000000-0000-0000-0000-000000000000", *stats(1, 0), conn=db, threshold="0.05"
        )


# --- through validate(): 1 broken record in 20 -------------------------------------------


def test_one_broken_record_in_twenty_passes_and_reaches_the_promotable_rows(
    db, make_run, crime_record
):
    run_id = make_run()
    records = [{**crime_record, "id": str(n)} for n in range(1, 21)]
    records[-1]["ward"] = "N/A"  # invalid_number
    ingest_stats = IngestStats(rows_fetched=20)
    write_page(db, run_id, DATASET_ID, records)

    validation_stats = validate(run_id, conn=db, now=NOW)
    result = run_quality_gate(run_id, ingest_stats, validation_stats, conn=db, threshold="0.05")

    assert (result.failed, result.checked, result.rate, result.passed) == (
        1, 20, Fraction(1, 20), True,
    )
    promotable = db.execute("SELECT source_id FROM bronze.promotable_records").fetchall()
    assert sorted(int(row[0]) for row in promotable) == list(range(1, 20))
