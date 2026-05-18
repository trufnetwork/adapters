"""EPS universe configuration for the Magnificent 7.

Stream layout per ticker (spec §3, §8):
  fmp_eps_<sym>_<conv>   — FMP primitive stream (raw FMP ingestion)
  yahoo_eps_<sym>_<conv> — Yahoo primitive stream (raw Yahoo ingestion)
  truf_eps_<sym>_<conv>  — Consensus stream (committed when ≥2 sources agree)

Stream IDs are deterministic: generated from a human-readable name via the
TN SDK so the same name always maps to the same on-chain stream ID.
"""
from __future__ import annotations

from trufnetwork_sdk_py.utils import generate_stream_id

MAG7: list[str] = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"]

# GAAP diluted for large-cap US tech; Non-GAAP diluted for NVDA/TSLA
# (FMP's epsActual follows this convention by construction)
EPS_CONVENTION: dict[str, str] = {
    "AAPL": "gaap",
    "MSFT": "gaap",
    "GOOGL": "gaap",
    "AMZN": "gaap",
    "META": "gaap",
    "NVDA": "nongaap",
    "TSLA": "nongaap",
}

# Source priority for reconciler — readings must be passed in this order
# so the highest-priority source's exact value is committed on settlement.
SOURCE_PRIORITY: list[str] = ["fmp", "yahoo"]


# ── Stream name helpers (human-readable → generate_stream_id input) ──────────

def truf_eps_stream_name(symbol: str) -> str:
    """Consensus stream name (committed after ≥2 sources agree)."""
    return f"truf_eps_{symbol.lower()}_{EPS_CONVENTION[symbol]}"


def fmp_eps_stream_name(symbol: str) -> str:
    """FMP primitive stream name (raw FMP ingestion)."""
    return f"fmp_eps_{symbol.lower()}_{EPS_CONVENTION[symbol]}"


def yahoo_eps_stream_name(symbol: str) -> str:
    """Yahoo primitive stream name (raw Yahoo ingestion)."""
    return f"yahoo_eps_{symbol.lower()}_{EPS_CONVENTION[symbol]}"


# ── Deterministic TN stream IDs ───────────────────────────────────────────────

def truf_eps_stream_id(symbol: str) -> str:
    return generate_stream_id(truf_eps_stream_name(symbol))


def fmp_eps_stream_id(symbol: str) -> str:
    return generate_stream_id(fmp_eps_stream_name(symbol))


def yahoo_eps_stream_id(symbol: str) -> str:
    return generate_stream_id(yahoo_eps_stream_name(symbol))


# Pre-computed dicts — import these in flows instead of calling the functions
# per-symbol to avoid repeated hashing.
EPS_STREAM_IDS: dict[str, str] = {sym: truf_eps_stream_id(sym) for sym in MAG7}
FMP_EPS_STREAM_IDS: dict[str, str] = {sym: fmp_eps_stream_id(sym) for sym in MAG7}
YAHOO_EPS_STREAM_IDS: dict[str, str] = {sym: yahoo_eps_stream_id(sym) for sym in MAG7}
