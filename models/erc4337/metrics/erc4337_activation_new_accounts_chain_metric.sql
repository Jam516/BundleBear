{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'timeframe', 'chain']
)
}} 

WITH day_metrics AS (
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE,
    CHAIN,
    'day' AS TIMEFRAME,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_ACTIVATIONS
    {% if is_incremental() %}
    WHERE DATE >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
    {% else %}
    WHERE DATE > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '12 months'
    {% endif %}
    GROUP BY 
    1, 2, 3
)

, week_metrics AS (
    SELECT 
    TO_VARCHAR(DATE_TRUNC('week', DATE), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'week' AS TIMEFRAME,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_ACTIVATIONS
    {% if is_incremental() %}
    WHERE DATE >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '3 week'
    {% else %}
    WHERE DATE > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '12 months'
    {% endif %}
    GROUP BY 
    1, 2, 3
)

, month_metrics AS (
    SELECT 
    TO_VARCHAR(DATE_TRUNC('month', DATE), 'YYYY-MM-DD') as DATE,
    CHAIN,
    'month' AS TIMEFRAME,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_ACTIVATIONS
    {% if is_incremental() %}
    WHERE DATE >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 month'
    {% else %}
    WHERE DATE > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '12 months'
    {% endif %}
    GROUP BY 
    1, 2, 3
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