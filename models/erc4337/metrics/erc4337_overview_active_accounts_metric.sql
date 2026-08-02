{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'timeframe', 'chain']
)
}}

WITH day_metrics AS (
SELECT 
TO_VARCHAR(date_trunc('day', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
'day' AS TIMEFRAME,
chain,
COUNT(DISTINCT SENDER) as num_accounts
FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
{% if is_incremental() %}
WHERE BLOCK_TIME >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
{% else %}
WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
{% endif %}
GROUP BY 1,2,3
)

, week_metrics AS (
SELECT 
TO_VARCHAR(date_trunc('week', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
'week' AS TIMEFRAME,
chain,
COUNT(DISTINCT SENDER) as num_accounts
FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
{% if is_incremental() %}
WHERE BLOCK_TIME >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week'
{% else %}
WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
{% endif %}
GROUP BY 1,2,3
)

, month_metrics AS (
SELECT 
TO_VARCHAR(date_trunc('month', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
'month' AS TIMEFRAME,
chain,
COUNT(DISTINCT SENDER) as num_accounts
FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
{% if is_incremental() %}
WHERE BLOCK_TIME >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
{% else %}
WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
{% endif %}
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