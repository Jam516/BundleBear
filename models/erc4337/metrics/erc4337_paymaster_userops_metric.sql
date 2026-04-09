{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'timeframe', 'chain', 'paymaster_name']
)
}}

WITH day_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('day', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'day' AS TIMEFRAME,
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(date_trunc('day', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    'all' AS CHAIN,
    'day' AS TIMEFRAME,
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    GROUP BY 1,2,3,4
)

, week_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('week', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'week' AS TIMEFRAME,
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(date_trunc('week', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    'all' AS CHAIN,
    'week' AS TIMEFRAME,
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    GROUP BY 1,2,3,4
)

, month_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('month', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'month' AS TIMEFRAME,
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(date_trunc('month', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    'all' AS CHAIN,
    'month' AS TIMEFRAME,
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    GROUP BY 1,2,3,4
)

SELECT
DATE,
TIMEFRAME,
CHAIN,
PAYMASTER_NAME,
NUM_USEROPS
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
PAYMASTER_NAME,
NUM_USEROPS
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
PAYMASTER_NAME,
NUM_USEROPS
FROM month_metrics