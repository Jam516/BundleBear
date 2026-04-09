{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'timeframe', 'chain', 'paymaster_type']
)
}}

WITH day_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('day', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'day' AS TIMEFRAME,
    CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
         WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
         ELSE PAYMASTER_TYPE
    END AS PAYMASTER_TYPE,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    AND PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(date_trunc('day', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    'all' AS CHAIN,
    'day' AS TIMEFRAME,
    CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
         WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
         ELSE PAYMASTER_TYPE
    END AS PAYMASTER_TYPE,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    AND PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    GROUP BY 1,2,3,4
)

, week_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('week', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'week' AS TIMEFRAME,
    CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
         WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
         ELSE PAYMASTER_TYPE
    END AS PAYMASTER_TYPE,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    AND PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(date_trunc('week', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    'all' AS CHAIN,
    'week' AS TIMEFRAME,
    CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
         WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
         ELSE PAYMASTER_TYPE
    END AS PAYMASTER_TYPE,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    AND PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    GROUP BY 1,2,3,4
)

, month_metrics AS (
    SELECT 
    TO_VARCHAR(date_trunc('month', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'month' AS TIMEFRAME,
    CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
         WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
         ELSE PAYMASTER_TYPE
    END AS PAYMASTER_TYPE,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    AND PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(date_trunc('month', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    'all' AS CHAIN,
    'month' AS TIMEFRAME,
    CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
         WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
         ELSE PAYMASTER_TYPE
    END AS PAYMASTER_TYPE,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
    {% else %}
    WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
    {% endif %}
    AND PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    GROUP BY 1,2,3,4
)

SELECT
DATE,
TIMEFRAME,
CHAIN,
PAYMASTER_TYPE,
GAS_SPENT
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
PAYMASTER_TYPE,
GAS_SPENT
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
PAYMASTER_TYPE,
GAS_SPENT
FROM month_metrics