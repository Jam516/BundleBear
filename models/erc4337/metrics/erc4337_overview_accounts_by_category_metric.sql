{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

WITH day_metrics AS (
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
    CHAIN,
    'day' AS TIMEFRAME,
    CASE WHEN NUM_OPS = 1 THEN '01 UserOp'
    WHEN NUM_OPS > 1 AND NUM_OPS <= 10 THEN '02-10 UserOps'
    ELSE 'More than 10 UserOps'
    END AS CATEGORY,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM (
        SELECT 
        date_trunc('day', BLOCK_TIME) AS DATE,
        CHAIN,
        SENDER,
        count(OP_HASH) AS NUM_OPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        GROUP BY 1,2,3
    )
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
    'all' AS CHAIN,
    'day' AS TIMEFRAME,
    CASE WHEN NUM_OPS = 1 THEN '01 UserOp'
    WHEN NUM_OPS > 1 AND NUM_OPS <= 10 THEN '02-10 UserOps'
    ELSE 'More than 10 UserOps'
    END AS CATEGORY,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM (
        SELECT 
        date_trunc('day', BLOCK_TIME) AS DATE,
        SENDER,
        count(OP_HASH) AS NUM_OPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        GROUP BY 1,2
    )
    GROUP BY 1,2,3,4
)

, week_metrics AS (
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
    CHAIN,
    'week' AS TIMEFRAME,
    CASE WHEN NUM_OPS = 1 THEN '01 UserOp'
    WHEN NUM_OPS > 1 AND NUM_OPS <= 10 THEN '02-10 UserOps'
    ELSE 'More than 10 UserOps'
    END AS CATEGORY,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM (
        SELECT 
        date_trunc('week', BLOCK_TIME) AS DATE,
        CHAIN,
        SENDER,
        count(OP_HASH) AS NUM_OPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        GROUP BY 1,2,3
    )
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
    'all' AS CHAIN,
    'week' AS TIMEFRAME,
    CASE WHEN NUM_OPS = 1 THEN '01 UserOp'
    WHEN NUM_OPS > 1 AND NUM_OPS <= 10 THEN '02-10 UserOps'
    ELSE 'More than 10 UserOps'
    END AS CATEGORY,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM (
        SELECT 
        date_trunc('week', BLOCK_TIME) AS DATE,
        SENDER,
        count(OP_HASH) AS NUM_OPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        GROUP BY 1,2
    )
    GROUP BY 1,2,3,4
)

, month_metrics AS (
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
    CHAIN,
    'month' AS TIMEFRAME,
    CASE WHEN NUM_OPS = 1 THEN '01 UserOp'
    WHEN NUM_OPS > 1 AND NUM_OPS <= 10 THEN '02-10 UserOps'
    ELSE 'More than 10 UserOps'
    END AS CATEGORY,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM (
        SELECT 
        date_trunc('month', BLOCK_TIME) AS DATE,
        CHAIN,
        SENDER,
        count(OP_HASH) AS NUM_OPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        GROUP BY 1,2,3
    )
    GROUP BY 1,2,3,4

    UNION ALL
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
    'all' AS CHAIN,
    'month' AS TIMEFRAME,
    CASE WHEN NUM_OPS = 1 THEN '01 UserOp'
    WHEN NUM_OPS > 1 AND NUM_OPS <= 10 THEN '02-10 UserOps'
    ELSE 'More than 10 UserOps'
    END AS CATEGORY,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM (
        SELECT 
        date_trunc('month', BLOCK_TIME) AS DATE,
        SENDER,
        count(OP_HASH) AS NUM_OPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
        WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '24 months'
        GROUP BY 1,2
    )
    GROUP BY 1,2,3,4
)

SELECT
DATE,
TIMEFRAME,
CHAIN,
CATEGORY,
NUM_ACCOUNTS
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
CATEGORY,
NUM_ACCOUNTS
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
CATEGORY,
NUM_ACCOUNTS
FROM month_metrics