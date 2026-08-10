"""
Unit tests for skipping SEPA prices that have not moved since the previous day.

TN reads carry the last observation forward, so a day with no record resolves to
the newest record at or before it. That is what makes it safe to write a product
only on the days its price changes — and what makes a wrongly-dropped row
invisible until someone reads a date and gets a stale number. These tests pin the
two halves of that: unchanged prices go, everything else stays.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
from pandera.typing import DataFrame
import pytest

from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks.date_processing_tasks import (
    filter_unchanged_products,
    load_previous_daily_averages,
    previous_day,
)
from tsn_adapters.tasks.argentina.types import DateStr


def make_averages(rows: list[tuple[str, float]], date_str: str) -> DataFrame[SepaAvgPriceProductModel]:
    """Build a daily-averages frame from (product id, price) pairs."""
    return DataFrame[SepaAvgPriceProductModel](
        pd.DataFrame(
            {
                "id_producto": [product_id for product_id, _ in rows],
                "productos_descripcion": [f"Product {product_id}" for product_id, _ in rows],
                "productos_precio_lista_avg": [price for _, price in rows],
                "date": [date_str] * len(rows),
            }
        )
    )


@pytest.fixture
def mock_provider() -> MagicMock:
    return MagicMock(spec=ProductAveragesProvider)


# --- previous_day ---


@pytest.mark.parametrize(
    "date_str, expected",
    [
        ("2026-08-10", "2026-08-09"),
        ("2026-08-01", "2026-07-31"),  # month boundary
        ("2026-01-01", "2025-12-31"),  # year boundary
        ("2028-03-01", "2028-02-29"),  # leap day
    ],
)
def test_previous_day(date_str: str, expected: str):
    assert previous_day(DateStr(date_str)) == expected


# --- filter_unchanged_products ---


def test_keeps_only_the_products_whose_price_moved():
    yesterday = make_averages([("1", 10.0), ("2", 20.0), ("3", 30.0)], "2026-08-09")
    today = make_averages([("1", 10.0), ("2", 22.5), ("3", 30.0)], "2026-08-10")

    result = filter_unchanged_products.fn(
        daily_avg_df=today, previous_avg_df=yesterday, date_str=DateStr("2026-08-10")
    )

    assert result["id_producto"].tolist() == ["2"]
    assert result["productos_precio_lista_avg"].tolist() == [22.5]


def test_keeps_a_product_absent_from_the_previous_day():
    """A gap breaks the chain back to the last published value, so republish.

    The previous day's file is a stand-in for what is standing on chain, and it
    only stands in when the product is actually in it. Dropping the row here
    would bet that the value from two or more days ago still matches.
    """
    yesterday = make_averages([("1", 10.0)], "2026-08-09")
    today = make_averages([("1", 10.0), ("2", 20.0)], "2026-08-10")

    result = filter_unchanged_products.fn(
        daily_avg_df=today, previous_avg_df=yesterday, date_str=DateStr("2026-08-10")
    )

    assert result["id_producto"].tolist() == ["2"]


def test_keeps_everything_when_there_is_no_previous_day():
    today = make_averages([("1", 10.0), ("2", 20.0)], "2026-08-10")

    result = filter_unchanged_products.fn(
        daily_avg_df=today, previous_avg_df=None, date_str=DateStr("2026-08-10")
    )

    assert result["id_producto"].tolist() == ["1", "2"]


def test_keeps_everything_when_the_previous_day_is_empty():
    today = make_averages([("1", 10.0)], "2026-08-10")
    empty = make_averages([], "2026-08-09")

    result = filter_unchanged_products.fn(
        daily_avg_df=today, previous_avg_df=empty, date_str=DateStr("2026-08-10")
    )

    assert result["id_producto"].tolist() == ["1"]


def test_drops_every_row_when_nothing_moved():
    yesterday = make_averages([("1", 10.0), ("2", 20.0)], "2026-08-09")
    today = make_averages([("1", 10.0), ("2", 20.0)], "2026-08-10")

    result = filter_unchanged_products.fn(
        daily_avg_df=today, previous_avg_df=yesterday, date_str=DateStr("2026-08-10")
    )

    assert result.empty


def test_a_repeated_product_yesterday_cannot_duplicate_todays_rows():
    """The previous day's file is joined on id_producto, which is not a key there.

    Two rows for one product would fan a single row of today's data into two on
    the merge, and the row count would silently grow instead of shrink.
    """
    yesterday = make_averages([("1", 10.0), ("1", 99.0)], "2026-08-09")
    today = make_averages([("1", 11.0)], "2026-08-10")

    result = filter_unchanged_products.fn(
        daily_avg_df=today, previous_avg_df=yesterday, date_str=DateStr("2026-08-10")
    )

    assert len(result) == 1
    assert result["productos_precio_lista_avg"].tolist() == [11.0]


def test_a_tiny_move_is_still_a_move():
    """Prices are compared exactly. A tolerance would swallow real movement."""
    yesterday = make_averages([("1", 10.0)], "2026-08-09")
    today = make_averages([("1", 10.000001)], "2026-08-10")

    result = filter_unchanged_products.fn(
        daily_avg_df=today, previous_avg_df=yesterday, date_str=DateStr("2026-08-10")
    )

    assert result["id_producto"].tolist() == ["1"]


def test_the_surviving_rows_keep_their_original_shape():
    """Whatever survives goes on to the descriptor join untouched."""
    yesterday = make_averages([("1", 10.0)], "2026-08-09")
    today = make_averages([("1", 12.0), ("2", 20.0)], "2026-08-10")

    result = filter_unchanged_products.fn(
        daily_avg_df=today, previous_avg_df=yesterday, date_str=DateStr("2026-08-10")
    )

    assert set(result.columns) == set(today.columns), "the merge must not leak its comparison column"
    assert result["date"].tolist() == ["2026-08-10", "2026-08-10"]


# --- load_previous_daily_averages ---


@pytest.mark.asyncio
async def test_load_previous_returns_none_when_the_day_is_missing(mock_provider: MagicMock):
    mock_provider.exists.return_value = False

    result = await load_previous_daily_averages.fn(provider=mock_provider, date_str=DateStr("2026-08-10"))

    assert result is None
    mock_provider.exists.assert_called_once_with("2026-08-09")


@pytest.mark.asyncio
async def test_load_previous_reads_the_day_before(mock_provider: MagicMock):
    mock_provider.exists.return_value = True
    yesterday = make_averages([("1", 10.0)], "2026-08-09")

    with patch(
        "tsn_adapters.tasks.argentina.tasks.date_processing_tasks.load_daily_averages",
        new_callable=AsyncMock,
    ) as mock_load:
        mock_load.return_value = yesterday
        result = await load_previous_daily_averages.fn(provider=mock_provider, date_str=DateStr("2026-08-10"))

    assert result is yesterday
    mock_load.assert_awaited_once_with(provider=mock_provider, date_str="2026-08-09")
