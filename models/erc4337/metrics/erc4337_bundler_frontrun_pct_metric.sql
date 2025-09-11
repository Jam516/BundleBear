{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

WITH day_metrics AS (
    WITH failed_ops AS (    
    SELECT 
    date_trunc('day', BLOCK_TIME) as DATE,
    CHAIN,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES_FAILED
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_FAILED_VALIDATION_OPS
    GROUP BY 1,2
    ),
    
    all_ops AS (
    SELECT 
    date_trunc('day', BLOCK_TIME) as DATE,
    CHAIN,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES_ALL
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ENTRYPOINT_TRANSACTIONS
    GROUP BY 1,2
    )
    
    SELECT
    TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
    f.CHAIN,
    'day' AS TIMEFRAME,
    100 * NUM_BUNDLES_FAILED/NUM_BUNDLES_ALL AS PCT_FRONTRUN
    FROM all_ops a
    INNER JOIN failed_ops f 
    ON a.DATE = f.DATE
    AND a.CHAIN = f.CHAIN
    

    UNION ALL 
    SELECT
    TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
    'all' AS CHAIN,
    'day' AS TIMEFRAME,
    100 * SUM(NUM_BUNDLES_FAILED)/SUM(NUM_BUNDLES_ALL) AS PCT_FRONTRUN
    FROM all_ops a
    INNER JOIN failed_ops f 
    ON a.DATE = f.DATE
    AND a.CHAIN = f.CHAIN
    GROUP BY 1,2,3
)

, week_metrics AS (
    WITH failed_ops AS (    
    SELECT 
    date_trunc('week', BLOCK_TIME) as DATE,
    CHAIN,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES_FAILED
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_FAILED_VALIDATION_OPS
    GROUP BY 1,2
    ),
    
    all_ops AS (
    SELECT 
    date_trunc('week', BLOCK_TIME) as DATE,
    CHAIN,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES_ALL
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ENTRYPOINT_TRANSACTIONS
    GROUP BY 1,2
    )
    
    SELECT
    TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
    f.CHAIN,
    'week' AS TIMEFRAME,
    100 * NUM_BUNDLES_FAILED/NUM_BUNDLES_ALL AS PCT_FRONTRUN
    FROM all_ops a
    INNER JOIN failed_ops f 
    ON a.DATE = f.DATE
    AND a.CHAIN = f.CHAIN
    

    UNION ALL 
    SELECT
    TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
    'all' AS CHAIN,
    'week' AS TIMEFRAME,
    100 * SUM(NUM_BUNDLES_FAILED)/SUM(NUM_BUNDLES_ALL) AS PCT_FRONTRUN
    FROM all_ops a
    INNER JOIN failed_ops f 
    ON a.DATE = f.DATE
    AND a.CHAIN = f.CHAIN
    GROUP BY 1,2,3
)

, month_metrics AS (
    WITH failed_ops AS (    
    SELECT 
    date_trunc('month', BLOCK_TIME) as DATE,
    CHAIN,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES_FAILED
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_FAILED_VALIDATION_OPS
    GROUP BY 1,2
    ),
    
    all_ops AS (
    SELECT 
    date_trunc('month', BLOCK_TIME) as DATE,
    CHAIN,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES_ALL
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ENTRYPOINT_TRANSACTIONS
    GROUP BY 1,2
    )
    
    SELECT
    TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
    f.CHAIN,
    'month' AS TIMEFRAME,
    100 * NUM_BUNDLES_FAILED/NUM_BUNDLES_ALL AS PCT_FRONTRUN
    FROM all_ops a
    INNER JOIN failed_ops f 
    ON a.DATE = f.DATE
    AND a.CHAIN = f.CHAIN
    

    UNION ALL 
    SELECT
    TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
    'all' AS CHAIN,
    'month' AS TIMEFRAME,
    100 * SUM(NUM_BUNDLES_FAILED)/SUM(NUM_BUNDLES_ALL) AS PCT_FRONTRUN
    FROM all_ops a
    INNER JOIN failed_ops f 
    ON a.DATE = f.DATE
    AND a.CHAIN = f.CHAIN
    GROUP BY 1,2,3
)

SELECT
DATE,
TIMEFRAME,
CHAIN,
PCT_FRONTRUN
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
PCT_FRONTRUN
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
PCT_FRONTRUN
FROM month_metrics