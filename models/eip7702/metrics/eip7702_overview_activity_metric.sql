{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'timeframe', 'chain']
)
}}

WITH day_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('day', BLOCK_DATE), 'YYYY-MM-DD') AS DATE,
    'day' AS TIMEFRAME,
    CHAIN,
    COUNT(*) AS NUM_AUTHORIZATIONS,
    COUNT(DISTINCT TX_HASH) AS NUM_SET_CODE_TXNS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ALL_AUTHORIZATIONS
    {% if is_incremental() %}
    WHERE BLOCK_DATE >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
    AND BLOCK_DATE < date_trunc('day', CURRENT_DATE())
    {% else %}
    WHERE BLOCK_DATE < date_trunc('day', CURRENT_DATE())
    {% endif %}
    GROUP BY 1,2,3
)

, week_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('week', BLOCK_DATE), 'YYYY-MM-DD') AS DATE,
    'week' AS TIMEFRAME,
    CHAIN,
    COUNT(*) AS NUM_AUTHORIZATIONS,
    COUNT(DISTINCT TX_HASH) AS NUM_SET_CODE_TXNS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ALL_AUTHORIZATIONS
    {% if is_incremental() %}
    WHERE BLOCK_DATE >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week'
    AND BLOCK_DATE < date_trunc('week', CURRENT_DATE())
    {% else %}
    WHERE BLOCK_DATE < date_trunc('week', CURRENT_DATE())
    {% endif %}
    GROUP BY 1,2,3
)

, month_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('month', BLOCK_DATE), 'YYYY-MM-DD') AS DATE,
    'month' AS TIMEFRAME,
    CHAIN,
    COUNT(*) AS NUM_AUTHORIZATIONS,
    COUNT(DISTINCT TX_HASH) AS NUM_SET_CODE_TXNS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ALL_AUTHORIZATIONS
    {% if is_incremental() %}
    WHERE BLOCK_DATE >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
    AND BLOCK_DATE < date_trunc('month', CURRENT_DATE())
    {% else %}
    WHERE BLOCK_DATE < date_trunc('month', CURRENT_DATE())
    {% endif %}
    GROUP BY 1,2,3 
)

SELECT
DATE,
TIMEFRAME,
CHAIN,
NUM_AUTHORIZATIONS,
NUM_SET_CODE_TXNS
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
NUM_AUTHORIZATIONS,
NUM_SET_CODE_TXNS
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
NUM_AUTHORIZATIONS,
NUM_SET_CODE_TXNS
FROM month_metrics