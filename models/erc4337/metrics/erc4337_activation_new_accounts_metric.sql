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
        DATE,
        CHAIN,
        PROVIDER,
        COUNT(SENDER) AS NUM_ACCOUNTS,
        ROW_NUMBER() OVER(PARTITION BY DATE, CHAIN ORDER BY COUNT(SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_ACTIVATIONS
        WHERE DATE > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '12 months'
        GROUP BY 
        1, 2, 3
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        CHAIN,
        'day' AS TIMEFRAME,
        CASE WHEN RN <= 9 THEN PROVIDER ELSE 'Other' END AS PROVIDER,
        SUM(NUM_ACCOUNTS) AS NUM_ACCOUNTS
        FROM 
        RankedProjects
        GROUP BY 
        1, 2,3,4
    )

    , agg_data AS (
        WITH RankedProjects AS (
        SELECT 
        DATE,
        PROVIDER,
        COUNT(SENDER) AS NUM_ACCOUNTS,
        ROW_NUMBER() OVER(PARTITION BY DATE ORDER BY COUNT(SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_ACTIVATIONS
        WHERE DATE > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '12 months'
        GROUP BY 
        1, 2
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        'all' AS CHAIN,
        'day' AS TIMEFRAME,
        CASE WHEN RN <= 9 THEN PROVIDER ELSE 'Other' END AS PROVIDER,
        SUM(NUM_ACCOUNTS) AS NUM_ACCOUNTS
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
        DATE_TRUNC('week', DATE) AS DATE,
        CHAIN,
        PROVIDER,
        COUNT(SENDER) AS NUM_ACCOUNTS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('week', DATE), CHAIN ORDER BY COUNT(SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_ACTIVATIONS
        WHERE DATE > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '12 months'
        GROUP BY 
        1, 2, 3
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        CHAIN,
        'week' AS TIMEFRAME,
        CASE WHEN RN <= 9 THEN PROVIDER ELSE 'Other' END AS PROVIDER,
        SUM(NUM_ACCOUNTS) AS NUM_ACCOUNTS
        FROM 
        RankedProjects
        GROUP BY 
        1, 2,3,4
    )

    , agg_data AS (
        WITH RankedProjects AS (
        SELECT 
        DATE_TRUNC('week', DATE) AS DATE,
        PROVIDER,
        COUNT(SENDER) AS NUM_ACCOUNTS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('week', DATE) ORDER BY COUNT(SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_ACTIVATIONS
        WHERE DATE > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '12 months'
        GROUP BY 
        1, 2
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        'all' AS CHAIN,
        'week' AS TIMEFRAME,
        CASE WHEN RN <= 9 THEN PROVIDER ELSE 'Other' END AS PROVIDER,
        SUM(NUM_ACCOUNTS) AS NUM_ACCOUNTS
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
        DATE_TRUNC('month', DATE) AS DATE,
        CHAIN,
        PROVIDER,
        COUNT(SENDER) AS NUM_ACCOUNTS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('month', DATE), CHAIN ORDER BY COUNT(SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_ACTIVATIONS
        WHERE DATE > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '12 months'
        GROUP BY 
        1, 2, 3
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        CHAIN,
        'month' AS TIMEFRAME,
        CASE WHEN RN <= 9 THEN PROVIDER ELSE 'Other' END AS PROVIDER,
        SUM(NUM_ACCOUNTS) AS NUM_ACCOUNTS
        FROM 
        RankedProjects
        GROUP BY 
        1, 2,3,4
    )

    , agg_data AS (
        WITH RankedProjects AS (
        SELECT 
        DATE_TRUNC('month', DATE) AS DATE,
        PROVIDER,
        COUNT(SENDER) AS NUM_ACCOUNTS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('month', DATE) ORDER BY COUNT(SENDER) DESC) AS RN
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_ACTIVATIONS
        WHERE DATE > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '12 months'
        GROUP BY 
        1, 2
        )
        
        SELECT 
        TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, 
        'all' AS CHAIN,
        'month' AS TIMEFRAME,
        CASE WHEN RN <= 9 THEN PROVIDER ELSE 'Other' END AS PROVIDER,
        SUM(NUM_ACCOUNTS) AS NUM_ACCOUNTS
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
PROVIDER,
NUM_ACCOUNTS
FROM day_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
PROVIDER,
NUM_ACCOUNTS
FROM week_metrics

UNION ALL
SELECT
DATE,
TIMEFRAME,
CHAIN,
PROVIDER,
NUM_ACCOUNTS
FROM month_metrics