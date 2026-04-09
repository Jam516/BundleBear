{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'timeframe', 'chain', 'bundler_name']
)
}}

WITH day_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('day', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'day' AS TIMEFRAME,
    BUNDLER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
    AND BLOCK_TIME < DATE_TRUNC('day', CURRENT_DATE())
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    AND BLOCK_TIME < DATE_TRUNC('day', CURRENT_DATE())
    {% endif %}
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(date_trunc('day', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    'all' AS CHAIN,
    'day' AS TIMEFRAME,
    BUNDLER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
    AND BLOCK_TIME < DATE_TRUNC('day', CURRENT_DATE())
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    AND BLOCK_TIME < DATE_TRUNC('day', CURRENT_DATE())
    {% endif %}
    GROUP BY 1,2,3,4
)

, week_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('week', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'week' AS TIMEFRAME,
    BUNDLER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week'
    AND BLOCK_TIME < DATE_TRUNC('week', CURRENT_DATE())
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    AND BLOCK_TIME < DATE_TRUNC('week', CURRENT_DATE())
    {% endif %}
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(date_trunc('week', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    'all' AS CHAIN,
    'week' AS TIMEFRAME,
    BUNDLER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week'
    AND BLOCK_TIME < DATE_TRUNC('week', CURRENT_DATE())
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    AND BLOCK_TIME < DATE_TRUNC('week', CURRENT_DATE())
    {% endif %}
    GROUP BY 1,2,3,4
)

, month_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('month', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'month' AS TIMEFRAME,
    BUNDLER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
    AND BLOCK_TIME < DATE_TRUNC('month', CURRENT_DATE())
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    AND BLOCK_TIME < DATE_TRUNC('month', CURRENT_DATE())
    {% endif %}
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(date_trunc('month', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    'all' AS CHAIN,
    'month' AS TIMEFRAME,
    BUNDLER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
    AND BLOCK_TIME < DATE_TRUNC('month', CURRENT_DATE())
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    AND BLOCK_TIME < DATE_TRUNC('month', CURRENT_DATE())
    {% endif %}
    GROUP BY 1,2,3,4
)

SELECT
DATE,
TIMEFRAME,
CHAIN,
BUNDLER_NAME,
NUM_ACCOUNTS
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
BUNDLER_NAME,
NUM_ACCOUNTS
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
BUNDLER_NAME,
NUM_ACCOUNTS
FROM month_metrics