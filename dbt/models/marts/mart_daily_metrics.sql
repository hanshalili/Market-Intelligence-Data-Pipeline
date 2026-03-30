/*
  mart_daily_metrics.sql — Analytics mart: financial metrics for AAPL, TSLA, SPY

  Source:  stg_stock_prices (staging view)
  Purpose: Compute all financial metrics used in the Plotly dashboard.

  Materialisation: TABLE, partitioned by date, clustered by symbol.
  This means BigQuery will:
    • Only scan the relevant date partitions (e.g. last 30 days).
    • Further prune to the relevant symbol cluster within each partition.
  A dashboard query like "WHERE symbol='AAPL' AND date >= '2024-01-01'"
  will scan a tiny fraction of the full table.

  Metrics computed (all use SQL window functions over the symbol partition):
  ─────────────────────────────────────────────────────────────────────────
  Trend
    sma_20   — 20-day simple moving average (short-term trend)
    sma_50   — 50-day simple moving average (medium-term trend)
    sma_200  — 200-day simple moving average (long-term trend / "golden cross")

  Return
    daily_return       — % change vs previous trading day
    cumulative_return  — % gain from the first trading day in the dataset

  Risk
    rolling_volatility_20d — Annualised StdDev of daily returns over 20 days
                             (proxy for near-term risk)
    drawdown               — % below the trailing 52-week high (downside risk)

  Relative performance
    spy_daily_return     — SPY's daily return on the same date (benchmark)
    excess_return_vs_spy — Stock return minus SPY return (alpha proxy)
  ─────────────────────────────────────────────────────────────────────────
*/

WITH

-- Step 1: Pull clean prices from the staging view
prices AS (
    SELECT
        date,
        symbol,
        adjusted_close,
        volume
    FROM {{ ref('stg_stock_prices') }}
),

-- Step 2: Compute daily return and running metrics using window functions
returns AS (
    SELECT
        date,
        symbol,
        adjusted_close,
        volume,

        -- ── Daily return ──────────────────────────────────────────────────
        -- Percentage change vs the previous trading day's adjusted close.
        -- LAG(1) looks back exactly one row within the same symbol, ordered by date.
        ROUND(
            SAFE_DIVIDE(
                adjusted_close - LAG(adjusted_close) OVER (
                    PARTITION BY symbol ORDER BY date
                ),
                LAG(adjusted_close) OVER (
                    PARTITION BY symbol ORDER BY date
                )
            ) * 100,
            6
        ) AS daily_return,

        -- ── Trend: Simple Moving Averages ─────────────────────────────────
        -- ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW = rolling N-day window.
        -- AVG() produces NULL when fewer than N rows are available (e.g. first 19 days for SMA20).
        ROUND(
            AVG(adjusted_close) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS sma_20,

        ROUND(
            AVG(adjusted_close) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS sma_50,

        ROUND(
            AVG(adjusted_close) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS sma_200,

        -- ── Risk: Rolling Volatility ───────────────────────────────────────
        -- Annualised standard deviation of daily returns over a 20-day window.
        -- Multiply by SQRT(252) to annualise (252 trading days per year).
        -- Computed on the return series, so we need LAG inside the window —
        -- we do this in the next CTE instead.

        -- ── Cumulative return: base price ─────────────────────────────────
        -- First adjusted_close per symbol (used to compute cumulative return below)
        FIRST_VALUE(adjusted_close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS first_adjusted_close,

        -- ── Drawdown: rolling 52-week high ───────────────────────────────
        MAX(adjusted_close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS rolling_52w_high

    FROM prices
),

-- Step 3: Compute volatility from the return series (requires the lag values)
volatility AS (
    SELECT
        date,
        symbol,
        adjusted_close,
        volume,
        daily_return,
        sma_20,
        sma_50,
        sma_200,
        first_adjusted_close,
        rolling_52w_high,

        -- Cumulative return from the first row for this symbol
        ROUND(
            SAFE_DIVIDE(adjusted_close - first_adjusted_close, first_adjusted_close) * 100,
            4
        ) AS cumulative_return,

        -- Drawdown: how far below the rolling 52-week high
        ROUND(
            SAFE_DIVIDE(adjusted_close - rolling_52w_high, rolling_52w_high) * 100,
            4
        ) AS drawdown,

        -- Rolling 20-day annualised volatility
        -- STDDEV_SAMP over the last 20 daily_return values * SQRT(252)
        ROUND(
            STDDEV_SAMP(daily_return) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) * SQRT(252),
            6
        ) AS rolling_volatility_20d

    FROM returns
),

-- Step 4: Extract SPY returns to join as benchmark
spy_returns AS (
    SELECT
        date,
        daily_return AS spy_daily_return
    FROM volatility
    WHERE symbol = 'SPY'
),

-- Step 5: Join SPY benchmark onto all symbols and compute excess return
final AS (
    SELECT
        v.date,
        v.symbol,
        v.adjusted_close,
        v.volume,
        v.daily_return,
        v.cumulative_return,
        v.sma_20,
        v.sma_50,
        v.sma_200,
        v.rolling_volatility_20d,
        v.drawdown,
        spy.spy_daily_return,
        -- Excess return = stock return minus market return (simple alpha)
        ROUND(v.daily_return - spy.spy_daily_return, 6) AS excess_return_vs_spy,
        CURRENT_TIMESTAMP()                              AS transformed_at
    FROM volatility AS v
    LEFT JOIN spy_returns AS spy
        ON v.date = spy.date
    -- SPY is included as a row; its excess_return_vs_spy will be 0.0 by definition
    -- (SPY return minus SPY return = 0), which is correct and intentional.
)

SELECT * FROM final
