"""Multi-source EPS reconciler — pure business logic, no I/O.

Accepts N source readings and returns a settlement decision.
Adding a new data source requires no changes here.

Settlement rules (per settlement spec):
  SETTLED  — ≥2 readings agree within ±$0.01
             committed value = exact figure from the highest-priority
             (first) source in the agreeing group; no averaging applied
  DISPUTED — ≥2 readings available but all diverge beyond ±$0.01
  PENDING  — fewer than 2 readings available

Caller ordering contract: pass readings sorted by source priority
(primary source first, e.g. fmp → yahoo → edgar). The first reading in
the largest agreeing group determines the committed value.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

TOLERANCE = 0.01  # sources must agree within $0.01 to settle


@dataclass(frozen=True)
class SourceReading:
    source: str         # e.g. "fmp", "yahoo", "edgar"
    eps_actual: float
    retrieved_at: str   # ISO datetime string


@dataclass
class ReconcileResult:
    status: Literal["settled", "disputed", "pending"]
    value: Optional[float]       # exact figure; only set when settled
    sources_agree: list[str]
    sources_disagree: list[str]


def reconcile(readings: list[SourceReading]) -> ReconcileResult:
    """Determine settlement status from N source readings.

    Returns SETTLED  if ≥2 sources agree within ±TOLERANCE.
    Returns PENDING  if fewer than 2 sources have reported.
    Returns DISPUTED if all pairwise differences exceed TOLERANCE.
    """
    if len(readings) < 2:
        return ReconcileResult(
            status="pending",
            value=None,
            sources_agree=[r.source for r in readings],
            sources_disagree=[],
        )

    # Find the largest contiguous window (by sorted eps_actual) where
    # max - min <= TOLERANCE. Using a sorted window avoids transitive joins
    # (e.g. 1.00/1.01/1.02 would falsely settle under the pairwise approach).
    # Caller priority order is preserved by indexing back into `readings`.
    sorted_readings = sorted(readings, key=lambda r: r.eps_actual)
    best_group: list[SourceReading] = []
    best_ref_value: float = 0.0

    lo = 0
    for hi in range(len(sorted_readings)):
        while round(sorted_readings[hi].eps_actual - sorted_readings[lo].eps_actual, 10) > TOLERANCE:
            lo += 1
        window = sorted_readings[lo : hi + 1]
        if len(window) > len(best_group):
            best_group = window
            # Committed value = exact figure from the highest-priority source
            # in the window (earliest position in original `readings` list).
            best_ref_value = min(window, key=lambda r: readings.index(r)).eps_actual

    if len(best_group) >= 2:
        outliers = [r.source for r in readings if r not in best_group]
        return ReconcileResult(
            status="settled",
            value=best_ref_value,
            sources_agree=[r.source for r in best_group],
            sources_disagree=outliers,
        )

    return ReconcileResult(
        status="disputed",
        value=None,
        sources_agree=[],
        sources_disagree=[r.source for r in readings],
    )
