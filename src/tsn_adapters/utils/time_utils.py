"""
Utility functions for time and date conversions.
"""

import pandas as pd
from pandera.typing import Series


def convert_date_str_series_to_unix_ts(date_series: Series[str]) -> Series[int]:
    """
    Convert a Series of 'YYYY-MM-DD' date strings to UTC midnight Unix timestamps.

    Args:
        date_series: A pandas Series containing date strings in 'YYYY-MM-DD' format.

    Returns:
        A pandas Series containing integer Unix timestamps (seconds since epoch at UTC midnight).

    Raises:
        ValueError: If dates are outside the valid range (e.g., before 1970) or conversion fails.
    """
    if date_series.empty:
        return pd.Series([], dtype=int)

    # 1. Convert strings to datetime objects
    # errors='coerce' will turn unparseable dates into NaT (Not a Time)
    dt_series = pd.to_datetime(date_series, format="%Y-%m-%d", errors="coerce", utc=True)

    # Check if any dates failed to parse
    if dt_series.isnull().any():
        invalid_dates = date_series[dt_series.isnull()].unique().tolist()
        raise ValueError(f"Failed to parse date strings to datetime objects. Invalid values found: {invalid_dates}")

    # 2. Ensure midnight UTC (normalization happens implicitly with utc=True and only date info)
    # dt_series = dt_series.dt.normalize() # normalize() might not be needed if time info wasn't present

    # 3. Convert to Unix timestamp (seconds since epoch)
    # Get nanoseconds since epoch
    ns_timestamps = dt_series.astype("int64")

    # Convert to seconds (integer division)
    second_timestamps = ns_timestamps // 10**9

    # 4. Validate the range (basic sanity check: 1970-01-01 to 2100-01-01)
    min_valid_timestamp = 0
    max_valid_timestamp = 4102444800
    if second_timestamps.lt(min_valid_timestamp).any() or second_timestamps.gt(max_valid_timestamp).any():
        raise ValueError(
            f"Converted timestamps outside valid range (1970-2100): "
            f"min={second_timestamps.min()}, max={second_timestamps.max()}"
        )

    return second_timestamps
