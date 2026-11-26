from datetime import date
from decimal import Decimal
from types import ModuleType
from uuid import uuid4

import pytest

from pipeline import __main__ as cli
from pipeline.db import get_connection
from pipeline.ingestion.ingest import IngestStats
from pipeline.quality.gate import GateResult, QualityGateError
from pipeline.quality.validate import ValidationStats
from pipeline.runs import start_run
from pipeline.silver.build import build_silver


@pytest.fixture
def use_test_database(db, monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", db.info.dbname)
    return db


def test_run_tracks_success_and_publication_order(use_test_database, monkeypatch):
    db = use_test_database
    calls = []
    interrupted = start_run("incremental", conn=db)

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
    assert db.execute(
        "SELECT status, error_message, finished_at IS NOT NULL"
        " FROM ops.pipeline_runs WHERE run_id = %s",
        (interrupted,),
    ).fetchone() == ("failed", "Previous pipeline process exited before finishing", True)


def test_cli_saves_quality_counts_and_stops_before_publication(use_test_database, monkeypatch):
    def fail_gate(*args, **kwargs):
        raise QualityGateError(GateResult(checked=20, failed=7, threshold=Decimal("0.05")))

    def no_publication(run_id):
        pytest.fail("The quality gate must block silver and gold")

    monkeypatch.setattr(cli, "ingest", lambda *args, **kwargs: IngestStats(rows_fetched=20))
    monkeypatch.setattr(
        cli, "validate", lambda *args, **kwargs: ValidationStats(checked=20, valid=13, invalid=7)
    )
    monkeypatch.setattr(cli, "run_quality_gate", fail_gate)
    monkeypatch.setattr(cli, "_publication_steps", lambda: (no_publication, no_publication))

    assert cli.main(["run", "--mode", "incremental"]) == 2
    with get_connection() as conn:
        assert conn.execute(
            "SELECT status, rows_fetched, rows_valid, rows_quarantined FROM ops.pipeline_runs"
        ).fetchone() == ("failed", 20, 13, 7)



def test_publication_steps_imports_the_real_silver_loader(monkeypatch):
    import sys

    gold = ModuleType("pipeline.gold")

    def load_gold(run_id):
        return run_id

    gold.load_gold = load_gold
    monkeypatch.setitem(sys.modules, "pipeline.gold", gold)
    assert cli._publication_steps() == (build_silver, load_gold)


def test_release_requires_investigator():
    with pytest.raises(SystemExit) as error:
        cli.main(["release", "--run-id", str(uuid4()), "--note", "Checked the batch"])

    assert error.value.code == 2


def test_release_cli_forwards_recheck(use_test_database, monkeypatch):
    run_id = uuid4()

    def release(actual_id, action, *, released_by, note, conn):
        assert (actual_id, action, released_by, note) == (
            run_id, "recheck", "Karim", "Updated validation rule"
        )
        return 3

    monkeypatch.setattr(cli, "release_run", release)

    assert cli.main([
        "release", "--run-id", str(run_id), "--action", "recheck",
        "--released-by", "Karim", "--note", "Updated validation rule",
    ]) == 0
