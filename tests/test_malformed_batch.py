"""End to end: a mixed batch of good and broken records from a mocked API through
ingest() and validate(). The same fixture file drives the quality-gate failure demo.
"""

import json
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import pytest
import responses
from responses import matchers

from pipeline.ingestion.bronze import CHICAGO
from pipeline.ingestion.client import SocrataClient
from pipeline.ingestion.ingest import ingest
from pipeline.quality.validate import validate

BATCH_FILE = Path(__file__).parent / "fixtures" / "malformed_batch.json"
BASE_URL = "https://api.test"
START, END = date(2026, 11, 1), date(2026, 11, 30)
NOW = datetime(2026, 11, 26, 12, 0, tzinfo=CHICAGO)


@pytest.fixture
def batch():
    return json.loads(BATCH_FILE.read_text(encoding="utf-8"))


@pytest.fixture
def ingested(db, make_run, batch):
    """Serve the batch as one API page, ingest it as a backfill, and return the run_id."""
    run_id = make_run()
    client = SocrataClient(BASE_URL, page_size=50, sleep=lambda seconds: None)
    where = "date >= '2026-11-01T00:00:00' AND date < '2026-12-01T00:00:00'"
    with responses.RequestsMock() as api:  # unregistered requests raise
        api.get(
            f"{BASE_URL}/resource/ijzp-q8t2.json",
            json=batch["records"],  # 20 < page size 50, so this is the last page
            match=[matchers.query_param_matcher({"$limit": "50", "$order": "id", "$where": where})],
        )
        stats = ingest(run_id, "backfill", START, END, conn=db, client=client)
    assert (stats.rows_fetched, stats.rows_inserted) == (20, 20)
    return run_id


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


def test_malformed_batch_failure_rate_is_well_over_the_gate(db, ingested):
    # 7 of 20 = 35%: the next branch's quality gate (5% by default) must reject this run.
    stats = validate(ingested, conn=db, now=NOW)
    assert stats.invalid / stats.checked == pytest.approx(0.35)
