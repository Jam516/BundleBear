{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'timeframe', 'chain', 'type']
)
}}

-- Scan EIP7702_ACTIONS once at daily grain, then roll up to day/week/month and 'all' chains.
-- Incremental runs recompute only recent, *complete* buckets (each lookback starts on a bucket boundary),
-- so merged rows always replace a bucket with its full count. The 3-day lookback covers the
-- 3-day reprocessing window of the upstream eip7702_actions model.
WITH daily AS (
    SELECT
    BLOCK_DATE,
    CHAIN,
    TYPE,
    COUNT(*) AS NUM_ACTIONS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= LEAST(
        DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day',
        DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week',
        DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
    )
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    AND BLOCK_TIME < DATE_TRUNC('day', CURRENT_DATE())
    GROUP BY 1,2,3
)

, daily_with_all AS (
    SELECT BLOCK_DATE, CHAIN, TYPE, NUM_ACTIONS FROM daily
    UNION ALL
    SELECT BLOCK_DATE, 'all' AS CHAIN, TYPE, SUM(NUM_ACTIONS) AS NUM_ACTIONS
    FROM daily
    GROUP BY 1,2,3
)

, day_metrics AS (
    SELECT
    TO_VARCHAR(BLOCK_DATE, 'YYYY-MM-DD') AS DATE,
    'day' AS TIMEFRAME,
    CHAIN,
    TYPE,
    NUM_ACTIONS
    FROM daily_with_all
    {% if is_incremental() %}
    WHERE BLOCK_DATE >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
    {% endif %}
)

, week_metrics AS (
    SELECT
    TO_VARCHAR(DATE_TRUNC('week', BLOCK_DATE), 'YYYY-MM-DD') AS DATE,
    'week' AS TIMEFRAME,
    CHAIN,
    TYPE,
    SUM(NUM_ACTIONS) AS NUM_ACTIONS
    FROM daily_with_all
    WHERE BLOCK_DATE < DATE_TRUNC('week', CURRENT_DATE())
    {% if is_incremental() %}
    AND BLOCK_DATE >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week'
    {% endif %}
    GROUP BY 1,2,3,4
)

, month_metrics AS (
    SELECT
    TO_VARCHAR(DATE_TRUNC('month', BLOCK_DATE), 'YYYY-MM-DD') AS DATE,
    'month' AS TIMEFRAME,
    CHAIN,
    TYPE,
    SUM(NUM_ACTIONS) AS NUM_ACTIONS
    FROM daily_with_all
    WHERE BLOCK_DATE < DATE_TRUNC('month', CURRENT_DATE())
    {% if is_incremental() %}
    AND BLOCK_DATE >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
    {% endif %}
    GROUP BY 1,2,3,4
)

SELECT DATE, TIMEFRAME, CHAIN, TYPE, NUM_ACTIONS FROM day_metrics
UNION ALL
SELECT DATE, TIMEFRAME, CHAIN, TYPE, NUM_ACTIONS FROM week_metrics
UNION ALL
SELECT DATE, TIMEFRAME, CHAIN, TYPE, NUM_ACTIONS FROM month_metrics
