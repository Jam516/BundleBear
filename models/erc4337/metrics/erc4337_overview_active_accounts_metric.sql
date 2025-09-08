{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

WITH day_metrics AS (
SELECT 
TO_VARCHAR(date_trunc('day', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
'day' AS TIMEFRAME,
chain,
COUNT(DISTINCT SENDER) as num_accounts
FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
GROUP BY 1,2,3
)

, week_metrics AS (
SELECT 
TO_VARCHAR(date_trunc('week', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
'week' AS TIMEFRAME,
chain,
COUNT(DISTINCT SENDER) as num_accounts
FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
GROUP BY 1,2,3
)

, month_metrics AS (
SELECT 
TO_VARCHAR(date_trunc('month', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
'month' AS TIMEFRAME,
chain,
COUNT(DISTINCT SENDER) as num_accounts
FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
GROUP BY 1,2,3
)

SELECT
DATE,
TIMEFRAME,
CHAIN,
NUM_ACCOUNTS
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
NUM_ACCOUNTS
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
NUM_ACCOUNTS
FROM month_metrics