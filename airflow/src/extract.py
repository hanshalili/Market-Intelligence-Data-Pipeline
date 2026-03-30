"""
extract.py — Pull daily adjusted OHLCV data from Alpha Vantage.

Design decisions:
- Uses TIME_SERIES_DAILY_ADJUSTED to get split/dividend-adjusted prices.
- Handles API errors and rate-limit responses explicitly.
- Returns a pandas DataFrame with a normalised schema — downstream steps
  do not need to know about the API response format.
- Idempotent: calling this multiple times for the same date returns the
  same data; no side effects outside the return value.
"""

import logging
import time
from datetime import date
from typing import Optional

import pandas as pd
import requests

from src.config import config

logger = logging.getLogger(__name__)

# Alpha Vantage column name mapping → our canonical schema
# Using TIME_SERIES_DAILY (free tier) — no adjusted_close; close is used for both
_AV_COLUMN_MAP = {
    "1. open": "open",
    "2. high": "high",
    "3. low": "low",
    "4. close": "close",
    "5. volume": "volume",
}


def fetch_daily_adjusted(
    symbol: str,
    output_size: str = "compact",  # 'compact' = last 100 days; 'full' = 20 years
) -> pd.DataFrame:
    """
    Fetch daily adjusted OHLCV data for a single ticker from Alpha Vantage.

    Args:
        symbol:      Ticker symbol, e.g. 'AAPL'.
        output_size: 'compact' for last 100 days (faster, cheaper on free tier).
                     'full' for complete history.

    Returns:
        DataFrame with columns: date, symbol, open, high, low, close,
        adjusted_close, volume. Sorted ascending by date.

    Raises:
        RuntimeError: If the API returns an error message or rate-limit notice.
        requests.HTTPError: On non-200 HTTP responses.
    """
    logger.info(
        "Fetching Alpha Vantage data for symbol=%s output_size=%s", symbol, output_size
    )

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": output_size,
        "apikey": config.alpha_vantage_api_key,
        "datatype": "json",
    }

    response = requests.get(
        config.alpha_vantage_base_url,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()

    # Alpha Vantage embeds errors inside a 200 response body
    if "Error Message" in payload:
        raise RuntimeError(
            f"Alpha Vantage error for {symbol}: {payload['Error Message']}"
        )

    if "Note" in payload:
        # Per-minute rate limit (5 req/min on free tier) — sleep and retry once
        logger.warning(
            "Alpha Vantage per-minute rate limit hit for %s. Sleeping 65s then retrying.",
            symbol,
        )
        time.sleep(65)
        response = requests.get(
            config.alpha_vantage_base_url, params=params, timeout=30
        )
        response.raise_for_status()
        payload = response.json()
        if "Note" in payload or "Information" in payload:
            raise RuntimeError(
                f"Alpha Vantage rate limit still active for {symbol} after retry. "
                "You may have exhausted your 25 req/day free quota. "
                "Wait until tomorrow or upgrade your API key."
            )

    if "Information" in payload:
        # Daily quota exhausted (25 req/day on free tier)
        raise RuntimeError(
            f"Alpha Vantage daily quota exhausted for {symbol}. "
            f"Message: {payload['Information']} "
            "The free tier allows 25 requests/day. "
            "Wait until tomorrow UTC midnight or upgrade at alphavantage.co/premium."
        )

    time_series_key = "Time Series (Daily)"
    if time_series_key not in payload:
        raise RuntimeError(
            f"Unexpected Alpha Vantage response for {symbol}. "
            f"Keys present: {list(payload.keys())}"
        )

    ts_data = payload[time_series_key]
    logger.info("Received %d rows for %s", len(ts_data), symbol)

    # Build a flat list of dicts, one per trading day
    rows = []
    for date_str, values in ts_data.items():
        row = {"date": date_str, "symbol": symbol.upper()}
        for av_col, our_col in _AV_COLUMN_MAP.items():
            row[our_col] = values.get(av_col)
        rows.append(row)

    df = pd.DataFrame(rows)
    df = _cast_schema(df)
    df = df.sort_values("date").reset_index(drop=True)

    logger.info(
        "Parsed %d rows for %s (date range: %s – %s)",
        len(df),
        symbol,
        df["date"].min(),
        df["date"].max(),
    )
    return df


def fetch_all_symbols(execution_date: Optional[date] = None) -> pd.DataFrame:
    """
    Fetch data for all configured symbols and combine into a single DataFrame.

    Respects the configured API sleep between requests to avoid rate limits.

    Args:
        execution_date: If provided, filter the result to only this date.
                        Useful for daily incremental loads.

    Returns:
        Combined DataFrame for all symbols, optionally filtered to execution_date.
    """
    frames = []

    for i, symbol in enumerate(config.symbols):
        if i > 0:
            logger.info(
                "Sleeping %ss between API calls (rate-limit protection)",
                config.api_sleep_seconds,
            )
            time.sleep(config.api_sleep_seconds)

        df = fetch_daily_adjusted(symbol, output_size="compact")
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)

    if execution_date is not None:
        date_str = execution_date.isoformat()
        combined = combined[combined["date"] == date_str].copy()
        logger.info(
            "Filtered to execution_date=%s — %d rows remain", date_str, len(combined)
        )

        if combined.empty:
            logger.warning(
                "No data found for execution_date=%s. "
                "This is expected on weekends and market holidays.",
                date_str,
            )

    return combined.reset_index(drop=True)


# ------------------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------------------


def _cast_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to their canonical types."""
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["adjusted_close"] = df["close"]  # free tier has no adjusted close
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    return df
