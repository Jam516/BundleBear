{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

WITH day_metrics AS (
    SELECT
    TO_VARCHAR(date_trunc('day', BLOCK_DATE), 'YYYY-MM-DD') as DATE,
    'day' AS TIMEFRAME,
    CHAIN,
    COUNT(DISTINCT FROM_ADDRESS) AS ACTIVE_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    AND BLOCK_TIME < date_trunc('day', CURRENT_DATE())
    GROUP BY 1,2,3
)

, week_metrics AS (
    SELECT
    TO_VARCHAR(date_trunc('week', BLOCK_DATE), 'YYYY-MM-DD') as DATE,
    'week' AS TIMEFRAME,
    CHAIN,
    COUNT(DISTINCT FROM_ADDRESS) AS ACTIVE_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    AND BLOCK_TIME < date_trunc('week', CURRENT_DATE())
    GROUP BY 1,2,3
)

, month_metrics AS (
    SELECT
    TO_VARCHAR(date_trunc('month', BLOCK_DATE), 'YYYY-MM-DD') as DATE,
    'month' AS TIMEFRAME,
    CHAIN,
    COUNT(DISTINCT FROM_ADDRESS) AS ACTIVE_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    AND BLOCK_TIME < date_trunc('month', CURRENT_DATE())
    GROUP BY 1,2,3
)

SELECT
DATE,
TIMEFRAME,
CHAIN,
ACTIVE_ACCOUNTS
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
ACTIVE_ACCOUNTS
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
ACTIVE_ACCOUNTS
FROM month_metrics