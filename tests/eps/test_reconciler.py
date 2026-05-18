"""Tests for the EPS multi-source reconciler."""

import pytest

from tsn_adapters.tasks.eps.reconciler import SourceReading, reconcile

NOW = "2025-01-01T00:00:00+00:00"


def reading(source: str, eps: float) -> SourceReading:
    return SourceReading(source=source, eps_actual=eps, retrieved_at=NOW)


def test_pending_when_single_source():
    result = reconcile([reading("fmp", 2.50)])
    assert result.status == "pending"
    assert result.value is None
    assert result.sources_agree == ["fmp"]


def test_pending_when_no_sources():
    result = reconcile([])
    assert result.status == "pending"
    assert result.value is None


def test_settled_when_two_sources_agree_exactly():
    result = reconcile([reading("fmp", 2.50), reading("yahoo", 2.50)])
    assert result.status == "settled"
    assert result.value == 2.50
    assert set(result.sources_agree) == {"fmp", "yahoo"}
    assert result.sources_disagree == []


def test_settled_within_tolerance():
    result = reconcile([reading("fmp", 2.50), reading("yahoo", 2.509)])
    assert result.status == "settled"


def test_settled_uses_primary_source_value_not_average():
    # FMP=2.50, Yahoo=2.51 — within tolerance; committed value must be FMP's exact figure
    result = reconcile([reading("fmp", 2.50), reading("yahoo", 2.51)])
    assert result.status == "settled"
    assert result.value == 2.50  # FMP's value, not 2.505


def test_disputed_when_sources_diverge():
    result = reconcile([reading("fmp", 2.50), reading("yahoo", 2.62)])
    assert result.status == "disputed"
    assert result.value is None
    assert set(result.sources_disagree) == {"fmp", "yahoo"}


def test_disputed_at_exact_tolerance_boundary():
    # Difference of exactly $0.01 should still settle (<=, not <)
    result = reconcile([reading("fmp", 2.50), reading("yahoo", 2.51)])
    assert result.status == "settled"


def test_disputed_just_over_tolerance():
    result = reconcile([reading("fmp", 2.50), reading("yahoo", 2.511)])
    assert result.status == "disputed"


def test_chained_values_do_not_falsely_settle():
    # 1.00 vs 1.01 is within tolerance, 1.01 vs 1.02 is within tolerance,
    # but 1.00 vs 1.02 exceeds tolerance — must not settle as a group of 3.
    result = reconcile([reading("fmp", 1.00), reading("yahoo", 1.01), reading("edgar", 1.02)])
    # Best window is any pair that fits (e.g. fmp+yahoo or yahoo+edgar), so settled,
    # but NOT all three sources in the agreeing group.
    assert result.status == "settled"
    assert len(result.sources_agree) == 2
    assert "edgar" not in result.sources_agree or "fmp" not in result.sources_agree


def test_chained_three_way_disputed():
    # All pairs diverge beyond tolerance — no group of ≥2 can settle.
    result = reconcile([reading("fmp", 1.00), reading("yahoo", 1.02), reading("edgar", 1.04)])
    assert result.status == "disputed"
