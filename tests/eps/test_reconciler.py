"""Tests for the EPS multi-source reconciler."""

from tsn_adapters.tasks.eps.reconciler import SourceReading, canonical_key, reconcile

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


# --- canonical_key tests (added with the trufnetwork/website#3936 fix) ---


def test_canonical_key_nvda_q1_fy27():
    """NVDA's fiscal Q1 FY27 ends April 26, 2026 — falls in calendar Q2 2026."""
    assert canonical_key("NVDA", "2026-04-26") == ("NVDA", 2026, 2)


def test_canonical_key_aapl_q2_fy26():
    """AAPL's fiscal Q2 FY26 ends March 28, 2026 — falls in calendar Q1 2026."""
    assert canonical_key("AAPL", "2026-03-28") == ("AAPL", 2026, 1)


def test_canonical_key_meta_q4_fy25():
    """META's Q4 FY25 ends December 31, 2025 — calendar Q4 2025."""
    assert canonical_key("META", "2025-12-31") == ("META", 2025, 4)


def test_canonical_key_collapses_yahoo_rounding():
    """FMP's exact fiscal-period-end and Yahoo's calendar-month-end rounded
    version both fall in the same calendar quarter — so they reduce to the
    same canonical key. This is the property that makes the reconciler join
    actually work after the #3936 fix.
    """
    fmp_key = canonical_key("NVDA", "2026-04-26")  # FMP's actual period-end
    yahoo_key = canonical_key("NVDA", "2026-04-30")  # Yahoo's month-end rounding
    assert fmp_key == yahoo_key == ("NVDA", 2026, 2)


def test_canonical_key_month_to_quarter_mapping():
    """Verify the (month - 1) // 3 + 1 formula across every month."""
    expectations = [
        (1, 1),
        (2, 1),
        (3, 1),  # Q1
        (4, 2),
        (5, 2),
        (6, 2),  # Q2
        (7, 3),
        (8, 3),
        (9, 3),  # Q3
        (10, 4),
        (11, 4),
        (12, 4),  # Q4
    ]
    for month, expected_q in expectations:
        date_str = f"2026-{month:02d}-15"
        sym, year, q = canonical_key("TEST", date_str)
        assert sym == "TEST"
        assert year == 2026
        assert q == expected_q, f"month {month} should map to Q{expected_q}, got Q{q}"


def test_canonical_key_distinguishes_symbols():
    """Same period-end, different symbols — different canonical keys."""
    assert canonical_key("NVDA", "2026-04-26") != canonical_key("AAPL", "2026-04-26")
