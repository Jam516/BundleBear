{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

WITH day_metrics AS (
    WITH segment_data AS (
        WITH RankedProjects AS (
        SELECT 
        DATE_TRUNC('day', u.BLOCK_TIME) AS DATE,
        CHAIN,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('day', u.BLOCK_TIME), CHAIN ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
        WHERE u.BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        GROUP BY 
        1, 2, 3
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        CHAIN,
        'day' AS TIMEFRAME,
        CASE WHEN RN <= 5 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_UNIQUE_SENDERS) AS NUM_UNIQUE_SENDERS
        FROM 
        RankedProjects
        GROUP BY 
        1, 2,3,4
    )

    , agg_data AS (
        WITH RankedProjects AS (
        SELECT 
        DATE_TRUNC('day', u.BLOCK_TIME) AS DATE,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('day', u.BLOCK_TIME) ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
        WHERE u.BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        GROUP BY 
        1, 2
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        'all' AS CHAIN,
        'day' AS TIMEFRAME,
        CASE WHEN RN <= 5 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_UNIQUE_SENDERS) AS NUM_UNIQUE_SENDERS
        FROM 
        RankedProjects
        GROUP BY 
        1, 2,3,4
    )

    SELECT * FROM segment_data
    UNION ALL
    SELECT * FROM agg_data
)

, week_metrics AS (
    WITH segment_data AS (
        WITH RankedProjects AS (
        SELECT 
        DATE_TRUNC('week', u.BLOCK_TIME) AS DATE,
        CHAIN,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('week', u.BLOCK_TIME), CHAIN ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
        WHERE u.BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        GROUP BY 
        1, 2, 3
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        CHAIN,
        'week' AS TIMEFRAME,
        CASE WHEN RN <= 5 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_UNIQUE_SENDERS) AS NUM_UNIQUE_SENDERS
        FROM 
        RankedProjects
        GROUP BY 
        1, 2,3,4
    )

    , agg_data AS (
        WITH RankedProjects AS (
        SELECT 
        DATE_TRUNC('week', u.BLOCK_TIME) AS DATE,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('week', u.BLOCK_TIME) ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
        WHERE u.BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        GROUP BY 
        1, 2
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        'all' AS CHAIN,
        'week' AS TIMEFRAME,
        CASE WHEN RN <= 5 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_UNIQUE_SENDERS) AS NUM_UNIQUE_SENDERS
        FROM 
        RankedProjects
        GROUP BY 
        1, 2,3,4
    )

    SELECT * FROM segment_data
    UNION ALL
    SELECT * FROM agg_data
)

, month_metrics AS (
    WITH segment_data AS (
        WITH RankedProjects AS (
        SELECT 
        DATE_TRUNC('month', u.BLOCK_TIME) AS DATE,
        CHAIN,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('month', u.BLOCK_TIME), CHAIN ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
        WHERE u.BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        GROUP BY 
        1, 2, 3
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        CHAIN,
        'month' AS TIMEFRAME,
        CASE WHEN RN <= 5 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_UNIQUE_SENDERS) AS NUM_UNIQUE_SENDERS
        FROM 
        RankedProjects
        GROUP BY 
        1, 2,3,4
    )

    , agg_data AS (
        WITH RankedProjects AS (
        SELECT 
        DATE_TRUNC('month', u.BLOCK_TIME) AS DATE,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('month', u.BLOCK_TIME) ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
        WHERE u.BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        GROUP BY 
        1, 2
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        'all' AS CHAIN,
        'month' AS TIMEFRAME,
        CASE WHEN RN <= 5 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_UNIQUE_SENDERS) AS NUM_UNIQUE_SENDERS
        FROM 
        RankedProjects
        GROUP BY 
        1, 2,3,4
    )

    SELECT * FROM segment_data
    UNION ALL
    SELECT * FROM agg_data
)

SELECT
DATE,
TIMEFRAME,
CHAIN,
PROJECT,
NUM_UNIQUE_SENDERS
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
PROJECT,
NUM_UNIQUE_SENDERS
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
PROJECT,
NUM_UNIQUE_SENDERS
FROM month_metrics