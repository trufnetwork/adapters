"""Tests for the EPS stream deployment flow."""

import pytest

from tsn_adapters.flows.eps.deploy_flow import EpsSourceDescriptor
from tsn_adapters.tasks.eps.config import (
    EPS_STREAM_IDS,
    FMP_EPS_STREAM_IDS,
    MAG7,
    YAHOO_EPS_STREAM_IDS,
)


def test_descriptor_produces_21_rows():
    df = EpsSourceDescriptor().get_descriptor()
    assert len(df) == 21  # 7 tickers × 3 stream types


def test_descriptor_stream_ids_are_unique():
    df = EpsSourceDescriptor().get_descriptor()
    assert df["stream_id"].nunique() == 21


def test_descriptor_covers_all_mag7():
    df = EpsSourceDescriptor().get_descriptor()
    assert set(df["source_id"].unique()) == set(MAG7)


def test_descriptor_has_all_three_source_types():
    df = EpsSourceDescriptor().get_descriptor()
    assert set(df["source_type"].unique()) == {"fmp_eps", "yahoo_eps", "truf_eps"}


def test_descriptor_stream_ids_match_config():
    df = EpsSourceDescriptor().get_descriptor()
    for symbol in MAG7:
        sym_rows = df[df["source_id"] == symbol]
        ids = set(sym_rows["stream_id"].tolist())
        expected = {FMP_EPS_STREAM_IDS[symbol], YAHOO_EPS_STREAM_IDS[symbol], EPS_STREAM_IDS[symbol]}
        assert ids == expected, f"Stream IDs mismatch for {symbol}"
