{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'timeframe', 'chain']
)
}}
WITH day_chain AS (
    SELECT
        date_trunc('day', BLOCK_TIME) AS DATE,
        CHAIN,
        SUM(CASE WHEN TYPE = 'erc-4337 userop' THEN 1 ELSE 0 END) AS NUM_USEROP_ACTIONS,
        COUNT(*) AS NUM_ACTIONS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
    WHERE
        {% if is_incremental() %}
        BLOCK_TIME >= DATEADD(DAY, -3, CURRENT_DATE())
        {% else %}
        BLOCK_TIME >= DATE('2025-05-05')
        {% endif %}
        AND BLOCK_TIME < CURRENT_DATE()
    GROUP BY 1,2
),

day_metrics AS (
    SELECT
        TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
        CHAIN,
        'day' AS TIMEFRAME,
        100 * NUM_USEROP_ACTIONS / NULLIF(NUM_ACTIONS, 0) AS PCT_USEROP_ACTIONS
    FROM day_chain

    UNION ALL

    SELECT
        TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
        'all' AS CHAIN,
        'day' AS TIMEFRAME,
        100 * SUM(NUM_USEROP_ACTIONS) / NULLIF(SUM(NUM_ACTIONS), 0) AS PCT_USEROP_ACTIONS
    FROM day_chain
    GROUP BY 1,2,3
),

week_chain AS (
    SELECT
        date_trunc('week', BLOCK_TIME) AS DATE,
        CHAIN,
        SUM(CASE WHEN TYPE = 'erc-4337 userop' THEN 1 ELSE 0 END) AS NUM_USEROP_ACTIONS,
        COUNT(*) AS NUM_ACTIONS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
    WHERE
        {% if is_incremental() %}
        BLOCK_TIME >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '1 week'
        {% else %}
        BLOCK_TIME >= DATE('2025-05-05')
        {% endif %}
        AND BLOCK_TIME < DATE_TRUNC('week', CURRENT_DATE())
    GROUP BY 1,2
),

week_metrics AS (
    SELECT
        TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
        CHAIN,
        'week' AS TIMEFRAME,
        100 * NUM_USEROP_ACTIONS / NULLIF(NUM_ACTIONS, 0) AS PCT_USEROP_ACTIONS
    FROM week_chain

    UNION ALL

    SELECT
        TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
        'all' AS CHAIN,
        'week' AS TIMEFRAME,
        100 * SUM(NUM_USEROP_ACTIONS) / NULLIF(SUM(NUM_ACTIONS), 0) AS PCT_USEROP_ACTIONS
    FROM week_chain
    GROUP BY 1,2,3
),

month_chain AS (
    SELECT
        date_trunc('month', BLOCK_TIME) AS DATE,
        CHAIN,
        SUM(CASE WHEN TYPE = 'erc-4337 userop' THEN 1 ELSE 0 END) AS NUM_USEROP_ACTIONS,
        COUNT(*) AS NUM_ACTIONS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
    WHERE
        {% if is_incremental() %}
        BLOCK_TIME >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '1 month'
        {% else %}
        BLOCK_TIME >= DATE('2025-05-05')
        {% endif %}
        AND BLOCK_TIME < DATE_TRUNC('month', CURRENT_DATE())
    GROUP BY 1,2
),

month_metrics AS (
    SELECT
        TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
        CHAIN,
        'month' AS TIMEFRAME,
        100 * NUM_USEROP_ACTIONS / NULLIF(NUM_ACTIONS, 0) AS PCT_USEROP_ACTIONS
    FROM month_chain

    UNION ALL

    SELECT
        TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
        'all' AS CHAIN,
        'month' AS TIMEFRAME,
        100 * SUM(NUM_USEROP_ACTIONS) / NULLIF(SUM(NUM_ACTIONS), 0) AS PCT_USEROP_ACTIONS
    FROM month_chain
    GROUP BY 1,2,3
)

SELECT
    DATE,
    TIMEFRAME,
    CHAIN,
    PCT_USEROP_ACTIONS
FROM day_metrics

UNION ALL
SELECT
    DATE,
    TIMEFRAME,
    CHAIN,
    PCT_USEROP_ACTIONS
FROM week_metrics

UNION ALL
SELECT
    DATE,
    TIMEFRAME,
    CHAIN,
    PCT_USEROP_ACTIONS
FROM month_metrics
