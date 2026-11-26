"""End to end: a mixed batch of good and broken records from a mocked API through
ingest(), validate() and the quality gate, which must reject it (7 of 20 broken).
"""

import json
from collections import Counter
from datetime import date, datetime
from fractions import Fraction
from pathlib import Path

import pytest
import responses
from responses import matchers

from pipeline.ingestion.bronze import CHICAGO
from pipeline.ingestion.client import SocrataClient
from pipeline.ingestion.expected_schema import EXPECTED_SCHEMA
from pipeline.ingestion.ingest import ingest
from pipeline.quality.gate import THRESHOLD_ENV, QualityGateError, run_quality_gate
from pipeline.quality.validate import validate

BATCH_FILE = Path(__file__).parent / "fixtures" / "malformed_batch.json"
BASE_URL = "https://api.test"
START, END = date(2026, 11, 1), date(2026, 11, 30)
NOW = datetime(2026, 11, 26, 12, 0, tzinfo=CHICAGO)


@pytest.fixture
def batch():
    return json.loads(BATCH_FILE.read_text(encoding="utf-8"))


@pytest.fixture
def ingest_result(db, make_run, batch):
    """Serve the batch as one API page, ingest it as a backfill: (run_id, IngestStats)."""
    run_id = make_run()
    client = SocrataClient(BASE_URL, page_size=50, sleep=lambda seconds: None)
    where = "date >= '2026-11-01T00:00:00' AND date < '2026-12-01T00:00:00'"
    with responses.RequestsMock() as api:  # unregistered requests raise
        columns = [{"fieldName": k, "dataTypeName": v} for k, v in EXPECTED_SCHEMA.items()]
        api.get(f"{BASE_URL}/api/views/ijzp-q8t2.json", json={"columns": columns})
        api.get(
            f"{BASE_URL}/resource/ijzp-q8t2.json",
            json=batch["records"],  # 20 < page size 50, so this is the last page
            match=[matchers.query_param_matcher({"$limit": "50", "$order": "id", "$where": where})],
        )
        stats = ingest(run_id, "backfill", START, END, conn=db, client=client)
    assert (stats.rows_fetched, stats.rows_inserted) == (20, 20)
    return run_id, stats


@pytest.fixture
def ingested(ingest_result):
    """The run_id of the ingested batch."""
    return ingest_result[0]


def test_malformed_batch_counts(db, ingested, batch):
    expected = batch["expected"]

    stats = validate(ingested, conn=db, now=NOW)

    assert stats.checked == expected["records"]
    assert stats.valid == expected["valid"]
    assert stats.invalid == expected["invalid"]
    assert sum(stats.rule_hits.values()) == expected["quarantine_rows"]


def test_malformed_batch_quarantines_exactly_the_broken_rules(db, ingested, batch):
    validate(ingested, conn=db, now=NOW)

    rows = db.execute("SELECT source_id, rule_name FROM ops.quarantine").fetchall()
    found = {}
    for source_id, rule_name in rows:
        found.setdefault(source_id, []).append(rule_name)
    expected = {source_id: info["rules"] for source_id, info in batch["expected"]["broken"].items()}
    assert {k: sorted(v) for k, v in found.items()} == {k: sorted(v) for k, v in expected.items()}


def test_malformed_batch_statuses(db, ingested, batch):
    validate(ingested, conn=db, now=NOW)

    statuses = Counter(
        row[0] for row in db.execute("SELECT validation_status FROM bronze.raw_records")
    )
    assert statuses == {"valid": 13, "invalid": 7}


def test_malformed_batch_fails_the_quality_gate(db, ingest_result, monkeypatch):
    monkeypatch.delenv(THRESHOLD_ENV, raising=False)  # the default threshold, 5%
    run_id, ingest_stats = ingest_result
    validation_stats = validate(run_id, conn=db, now=NOW)

    with pytest.raises(QualityGateError, match=r"7 of 20 records failed \(35\.00%\)") as error:
        run_quality_gate(run_id, ingest_stats, validation_stats, conn=db)

    assert error.value.result.rate == Fraction(7, 20)
    passed = db.execute(
        "SELECT passed FROM ops.quality_gate_results WHERE run_id = %s", (run_id,)
    ).fetchone()[0]
    assert passed is False


def test_failed_gate_keeps_bronze_and_quarantine_but_promotes_nothing(db, ingest_result):
    run_id, ingest_stats = ingest_result
    validation_stats = validate(run_id, conn=db, now=NOW)

    with pytest.raises(QualityGateError):
        run_quality_gate(run_id, ingest_stats, validation_stats, conn=db, threshold="0.05")

    def count(table):
        return db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

    # Everything stays for investigation, but none of the 13 valid rows may reach silver.
    assert count("bronze.raw_records") == 20
    assert count("bronze.raw_records WHERE validation_status = 'valid'") == 13
    assert count("ops.quarantine") == 8
    assert count("bronze.promotable_records") == 0
