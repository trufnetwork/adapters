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

    # Find the largest group of readings that all agree within TOLERANCE.
    # Iterating in caller priority order means the first maximum group uses
    # the highest-priority source as reference — its exact value is committed.
    best_group: list[SourceReading] = []
    best_ref_value: float = 0.0

    for ref in readings:
        group = [r for r in readings if round(abs(r.eps_actual - ref.eps_actual), 10) <= TOLERANCE]
        if len(group) > len(best_group):
            best_group = group
            best_ref_value = ref.eps_actual  # exact figure, no averaging

    if len(best_group) >= 2:
        outliers = [r.source for r in readings if r not in best_group]
        value = round(best_ref_value, 2)
        return ReconcileResult(
            status="settled",
            value=value,
            sources_agree=[r.source for r in best_group],
            sources_disagree=outliers,
        )

    return ReconcileResult(
        status="disputed",
        value=None,
        sources_agree=[],
        sources_disagree=[r.source for r in readings],
    )
