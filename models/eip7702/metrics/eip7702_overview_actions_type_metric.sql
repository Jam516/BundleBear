{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

WITH day_metrics AS (
    WITH segment_data AS (
        SELECT
        TO_VARCHAR(date_trunc('day', BLOCK_DATE), 'YYYY-MM-DD') as DATE,
        'day' AS TIMEFRAME,
        CHAIN,
        TYPE,
        COUNT(*) AS NUM_ACTIONS
        FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        AND BLOCK_TIME < date_trunc('day', CURRENT_DATE())
        GROUP BY 1,2,3,4
    )

    , agg_data AS (
        SELECT
        TO_VARCHAR(date_trunc('day', BLOCK_DATE), 'YYYY-MM-DD') as DATE,
        'day' AS TIMEFRAME,
        'all' AS CHAIN,
        TYPE,
        COUNT(*) AS NUM_ACTIONS
        FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        AND BLOCK_TIME < date_trunc('day', CURRENT_DATE())
        GROUP BY 1,2,3,4
    )

    SELECT * FROM segment_data
    UNION ALL
    SELECT * FROM agg_data
)

, week_metrics AS (
    WITH segment_data AS (
        SELECT
        TO_VARCHAR(date_trunc('week', BLOCK_DATE), 'YYYY-MM-DD') as DATE,
        'week' AS TIMEFRAME,
        CHAIN,
        TYPE,
        COUNT(*) AS NUM_ACTIONS
        FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        AND BLOCK_TIME < date_trunc('week', CURRENT_DATE())
        GROUP BY 1,2,3,4
    )

    , agg_data AS (
        SELECT
        TO_VARCHAR(date_trunc('week', BLOCK_DATE), 'YYYY-MM-DD') as DATE,
        'week' AS TIMEFRAME,
        'all' AS CHAIN,
        TYPE,
        COUNT(*) AS NUM_ACTIONS
        FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        AND BLOCK_TIME < date_trunc('week', CURRENT_DATE())
        GROUP BY 1,2,3,4
    )

    SELECT * FROM segment_data
    UNION ALL
    SELECT * FROM agg_data
)

, month_metrics AS (
    WITH segment_data AS (
        SELECT
        TO_VARCHAR(date_trunc('month', BLOCK_DATE), 'YYYY-MM-DD') as DATE,
        'month' AS TIMEFRAME,
        CHAIN,
        TYPE,
        COUNT(*) AS NUM_ACTIONS
        FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        AND BLOCK_TIME < date_trunc('month', CURRENT_DATE())
        GROUP BY 1,2,3,4
    )

    , agg_data AS (
        SELECT
        TO_VARCHAR(date_trunc('month', BLOCK_DATE), 'YYYY-MM-DD') as DATE,
        'month' AS TIMEFRAME,
        'all' AS CHAIN,
        TYPE,
        COUNT(*) AS NUM_ACTIONS
        FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        AND BLOCK_TIME < date_trunc('month', CURRENT_DATE())
        GROUP BY 1,2,3,4
    )

    SELECT * FROM segment_data
    UNION ALL
    SELECT * FROM agg_data
)

SELECT
DATE,
TIMEFRAME,
CHAIN,
TYPE,
NUM_ACTIONS
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
TYPE,
NUM_ACTIONS
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
TYPE,
NUM_ACTIONS
FROM month_metrics