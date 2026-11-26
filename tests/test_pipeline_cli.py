from datetime import date
from decimal import Decimal

import pytest

from pipeline import __main__ as cli
from pipeline.db import get_connection
from pipeline.ingestion.ingest import IngestStats
from pipeline.quality.gate import GateResult, QualityGateError
from pipeline.quality.validate import ValidationStats


@pytest.fixture
def use_test_database(db, monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", db.info.dbname)
    return db


def test_run_tracks_success_and_publication_order(use_test_database, monkeypatch):
    db = use_test_database
    calls = []

    def ingestion(run_id, mode, start, end, *, conn):
        calls.append("ingest")
        assert conn.execute(
            "SELECT status FROM ops.pipeline_runs WHERE run_id = %s", (run_id,)
        ).fetchone() == ("running",)
        return IngestStats(rows_fetched=3)

    def validation(run_id, *, conn):
        calls.append("validate")
        return ValidationStats(checked=3, valid=3)

    def gate(run_id, ingestion_stats, validation_stats, *, conn):
        calls.append("gate")

    monkeypatch.setattr(cli, "ingest", ingestion)
    monkeypatch.setattr(cli, "validate", validation)
    monkeypatch.setattr(cli, "run_quality_gate", gate)
    monkeypatch.setattr(
        cli, "_publication_steps",
        lambda: (lambda run_id: calls.append("silver"), lambda run_id: calls.append("gold")),
    )
    run_id = cli.run_pipeline("backfill", date(2025, 1, 1), date(2025, 1, 1))

    assert calls == ["ingest", "validate", "gate", "silver", "gold"]
    assert db.execute(
        "SELECT status, rows_fetched, rows_valid FROM ops.pipeline_runs WHERE run_id = %s",
        (run_id,),
    ).fetchone() == ("success", 3, 3)


def test_cli_saves_quality_counts_and_stops_before_publication(use_test_database, monkeypatch):
    def fail_gate(*args, **kwargs):
        raise QualityGateError(GateResult(checked=20, failed=7, threshold=Decimal("0.05")))

    def no_publication(run_id):
        pytest.fail("The quality gate must block silver and gold")

    monkeypatch.setattr(cli, "ingest", lambda *args, **kwargs: IngestStats(rows_fetched=20))
    monkeypatch.setattr(cli, "validate", lambda *args, **kwargs: ValidationStats())
    monkeypatch.setattr(cli, "run_quality_gate", fail_gate)
    monkeypatch.setattr(cli, "_publication_steps", lambda: (no_publication, no_publication))

    assert cli.main(["run", "--mode", "incremental"]) == 2
    with get_connection() as conn:
        assert conn.execute(
            "SELECT status, rows_fetched, rows_valid, rows_quarantined FROM ops.pipeline_runs"
        ).fetchone() == ("failed", 20, 13, 7)
