"""Quality gate: fail a run when too many of the records it checked are broken.

The pipeline runs these steps in this order, each with the same run_id:

    ingest -> validate -> run_quality_gate -> build_silver -> load_gold

run_quality_gate() saves its decision in ops.quality_gate_results and raises
QualityGateError when the run fails. build_silver() reads only
bronze.promotable_records: valid rows whose validating run passed the gate, or that a
person released after investigating (pipeline/quality/release.py).

Failure rate = failed / checked (CLAUDE.md, "Quality gate"), where

    failed  = ValidationStats.invalid + IngestStats.rows_quarantined
    checked = ValidationStats.checked + IngestStats.rows_quarantined

IngestStats.rows_quarantined counts records with no id: they never reached bronze, so
validate() didn't see them. Both sides count records, not rule hits.

The run fails only when the rate is strictly greater than the threshold; a rate equal
to it passes, so 1 failed in 20 against 0.05 passes. The rate and the threshold are
compared as exact fractions, so the result never depends on float rounding. If nothing
was checked, the gate passes. The default threshold (0.05) assumes daily runs of
several hundred records; in much smaller runs a single broken record can exceed it.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from uuid import UUID

import psycopg

from pipeline.ingestion.bronze import MISSING_SOURCE_ID_RULE
from pipeline.ingestion.ingest import IngestStats
from pipeline.quality.validate import ValidationStats

logger = logging.getLogger(__name__)

THRESHOLD_ENV = "QUALITY_MAX_FAILURE_RATE"
DEFAULT_THRESHOLD = Decimal("0.05")


@dataclass(frozen=True)
class GateResult:
    checked: int  # records checked, including ones quarantined at ingestion
    failed: int  # records that failed: distinct records, not rule hits
    threshold: Decimal
    rule_hits: dict[str, int] = field(default_factory=dict)  # broken rules, per rule_name

    @property
    def rate(self) -> Fraction:
        return Fraction(self.failed, self.checked) if self.checked else Fraction(0)

    @property
    def passed(self) -> bool:
        return self.rate <= Fraction(self.threshold)  # exact: no float rounding

    def describe(self, run_id: UUID | None = None) -> str:
        """One line for logs and error messages."""
        run = f" for run {run_id}" if run_id is not None else ""
        verdict, relation = ("passed", "within") if self.passed else ("failed", "above")
        text = (
            f"Quality gate {verdict}{run}: {self.failed} of {self.checked} records failed"
            f" ({float(self.rate):.2%}), {relation} {THRESHOLD_ENV}={self.threshold}"
            f" ({float(self.threshold):.2%})."
        )
        if self.rule_hits:
            hits = sorted(self.rule_hits.items(), key=lambda item: (-item[1], item[0]))
            text += " Broken rules: " + ", ".join(f"{name}={count}" for name, count in hits) + "."
        return text


class QualityGateError(Exception):
    """The run's failure rate is above the threshold; ``result`` holds the numbers."""

    def __init__(self, result: GateResult, run_id: UUID | None = None) -> None:
        self.result = result
        self.run_id = run_id
        super().__init__(result.describe(run_id))


def parse_threshold(value: str | float | Decimal) -> Decimal:
    """A failure-rate threshold from 0 to 1. 0: any failure fails; 1: never fails."""
    try:
        # str() first, so the float 0.05 becomes exactly Decimal('0.05').
        threshold = Decimal(str(value).strip())
    except InvalidOperation:
        threshold = None
    # is_finite() first: comparing NaN with < raises instead of returning False.
    if threshold is None or not threshold.is_finite() or not 0 <= threshold <= 1:
        raise ValueError(f"{THRESHOLD_ENV} must be a number from 0 to 1, got {value!r}")
    return threshold


def load_threshold() -> Decimal:
    """QUALITY_MAX_FAILURE_RATE from the environment; 0.05 when unset or blank."""
    raw = os.environ.get(THRESHOLD_ENV, "")
    return parse_threshold(raw) if raw.strip() else DEFAULT_THRESHOLD


def check_quality_gate(
    ingest_stats: IngestStats,
    validation_stats: ValidationStats,
    threshold: str | float | Decimal,
) -> GateResult:
    """Return the gate's numbers, or raise QualityGateError if the rate exceeds threshold.

    Pure: reads nothing and writes nothing. run_quality_gate() is the pipeline step.
    """
    result = _evaluate(ingest_stats, validation_stats, parse_threshold(threshold))
    if not result.passed:
        raise QualityGateError(result)
    return result


def run_quality_gate(
    run_id: UUID,
    ingest_stats: IngestStats,
    validation_stats: ValidationStats,
    *,
    conn: psycopg.Connection | None = None,
    threshold: str | float | Decimal | None = None,
) -> GateResult:
    """The pipeline step after validate(): save the decision, raise if the run failed.

    The decision is saved in ops.quality_gate_results either way, BEFORE raising, so a
    failed run's rows stay held (not in bronze.promotable_records) and can be released
    later. Running it again for the same run replaces the decision.

    ``threshold`` defaults to QUALITY_MAX_FAILURE_RATE (see load_threshold()). ``conn``
    is for tests; the decision is committed on it unless the caller has a transaction open.
    """
    threshold = load_threshold() if threshold is None else parse_threshold(threshold)
    result = _evaluate(ingest_stats, validation_stats, threshold)

    owns_conn = conn is None
    if conn is None:
        from pipeline.db import get_connection  # Student 3's module, not on main yet

        conn = get_connection()
    try:
        _save_decision(conn, run_id, result)
    finally:
        if owns_conn:
            conn.close()

    if not result.passed:
        error = QualityGateError(result, run_id)
        logger.error("%s", error)
        raise error
    logger.info("%s", result.describe(run_id))
    return result


def _evaluate(
    ingest_stats: IngestStats, validation_stats: ValidationStats, threshold: Decimal
) -> GateResult:
    no_id = ingest_stats.rows_quarantined  # quarantined at ingestion, never in bronze
    rule_hits = dict(validation_stats.rule_hits)
    if no_id:
        rule_hits[MISSING_SOURCE_ID_RULE] = no_id
    return GateResult(
        checked=validation_stats.checked + no_id,
        failed=validation_stats.invalid + no_id,
        threshold=threshold,
        rule_hits=rule_hits,
    )


def _save_decision(conn: psycopg.Connection, run_id: UUID, result: GateResult) -> None:
    # transaction(): commits on its own, in autocommit mode or not, unless the caller
    # already has a transaction open (then it is a savepoint and the caller commits).
    with conn.transaction():
        conn.execute(
            "INSERT INTO ops.quality_gate_results"
            " (run_id, checked, failed, threshold, passed, decided_at)"
            " VALUES (%s, %s, %s, %s, %s, now())"
            " ON CONFLICT (run_id) DO UPDATE SET"
            " checked = EXCLUDED.checked, failed = EXCLUDED.failed,"
            " threshold = EXCLUDED.threshold, passed = EXCLUDED.passed,"
            " decided_at = EXCLUDED.decided_at",
            (run_id, result.checked, result.failed, result.threshold, result.passed),
        )
