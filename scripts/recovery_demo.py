"""Exercise checkpoint recovery and the quality gate with an in-memory API."""

import argparse
import json
import logging
import time
from datetime import date, datetime
from pathlib import Path

from pipeline import __main__ as cli
from pipeline.ingestion.bronze import CHICAGO
from pipeline.ingestion.expected_schema import expected_columns
from pipeline.ingestion.ingest import ingest

START = date(2026, 11, 1)
END = date(2026, 11, 30)



def crime_record(record_id):
    return {
        "id": record_id, "case_number": f"DEMO-{record_id}",
        "date": "2026-11-01T00:00:00.000", "updated_on": "2026-11-01T00:15:00.000",
        "iucr": "1320", "primary_type": "CRIMINAL DAMAGE",
        "arrest": False, "domestic": False, "year": "2026",
    }


class CheckpointPages:
    def __init__(self, phase):
        self.phase = phase

    def fetch_columns(self):
        return expected_columns()

    def iter_pages(self, *, start_date, end_date, after_id):
        assert (start_date, end_date) == (START, END)
        if self.phase == "pause":
            assert after_id is None
            yield [crime_record("1")]
            print("CHECKPOINT_COMMITTED: 1", flush=True)
            time.sleep(120)
        else:
            assert after_id == 1
            print("RESUMED_FROM_CHECKPOINT: 1", flush=True)
            yield [crime_record("2")]


class MalformedPage:
    def fetch_columns(self):
        return expected_columns()

    def iter_pages(self, *, start_date, end_date, after_id):
        assert (start_date, end_date) == (START, END)
        assert after_id is None
        payload = json.loads(Path("/tmp/malformed_batch.json").read_text(encoding="utf-8"))
        yield payload["records"]


def prove_malformed_batch():
    validate = cli.validate

    def ingest_fixture(run_id, mode, start, end, *, conn):
        return ingest(run_id, mode, start, end, conn=conn, client=MalformedPage())

    def validate_fixture(run_id, *, conn):
        return validate(run_id, conn=conn, now=datetime(2026, 11, 26, 12, tzinfo=CHICAGO))

    def unexpected_publication(run_id):
        raise AssertionError("The quality gate must stop publication")

    cli.ingest = ingest_fixture
    cli.validate = validate_fixture
    cli._publication_steps = lambda: (unexpected_publication, unexpected_publication)
    return cli.main(["run", "--mode", "backfill", "--start", str(START), "--end", str(END)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("pause", "resume", "malformed"))
    phase = parser.parse_args().phase
    if phase == "malformed":
        return prove_malformed_batch()

    def ingest_fixture(run_id, mode, start, end, *, conn):
        return ingest(run_id, mode, start, end, conn=conn, client=CheckpointPages(phase))

    cli.ingest = ingest_fixture
    cli._publication_steps = lambda: (lambda run_id: None, lambda run_id: None)
    return cli.main(["run", "--mode", "backfill", "--start", str(START), "--end", str(END)])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
