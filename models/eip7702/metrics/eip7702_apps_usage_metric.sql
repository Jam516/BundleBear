{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

WITH day_metrics AS (
WITH seg_data AS (
    WITH project_counts AS (
      SELECT
        date_trunc('day', BLOCK_DATE) as DATE,
        CHAIN,
        COALESCE(ap.NAME, a.TO_ADDRESS, 'Contract Deployment') as PROJECT,
        COUNT(DISTINCT FROM_ADDRESS) AS NUM_WALLETS,
        ROW_NUMBER() OVER (PARTITION BY DATE, CHAIN ORDER BY NUM_WALLETS DESC) as rank
      FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS a
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_AUTHORIZED_CONTRACTS l
        ON a.AUTHORIZED_CONTRACT = l.ADDRESS
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_APPS ap 
        ON ap.ADDRESS = a.TO_ADDRESS
      WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        AND BLOCK_TIME < date_trunc('day', CURRENT_DATE())
      GROUP BY 1, 2, 3
    )
    SELECT
      TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
      CHAIN,
      CASE 
        WHEN rank <= 10 THEN PROJECT
        ELSE 'other'
      END AS PROJECT,
      SUM(NUM_WALLETS) AS NUM_UNIQUE_SENDERS
    FROM project_counts
    GROUP BY 1, 2, 3
)

, agg_data AS (
    WITH project_counts AS (
      SELECT
        date_trunc('day', BLOCK_DATE) as DATE,
        COALESCE(ap.NAME, a.TO_ADDRESS, 'Contract Deployment') as PROJECT,
        COUNT(DISTINCT FROM_ADDRESS) AS NUM_WALLETS,
        ROW_NUMBER() OVER (PARTITION BY DATE ORDER BY NUM_WALLETS DESC) as rank
      FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS a
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_AUTHORIZED_CONTRACTS l
        ON a.AUTHORIZED_CONTRACT = l.ADDRESS
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_APPS ap 
        ON ap.ADDRESS = a.TO_ADDRESS
      WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        AND BLOCK_TIME < date_trunc('day', CURRENT_DATE())
      GROUP BY 1, 2
    )
    SELECT
      TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
      'all' AS CHAIN,
      CASE 
        WHEN rank <= 10 THEN PROJECT
        ELSE 'other'
      END AS PROJECT,
      SUM(NUM_WALLETS) AS NUM_UNIQUE_SENDERS
    FROM project_counts
    GROUP BY 1, 2, 3
)

SELECT *, 'day' AS TIMEFRAME FROM seg_data 
UNION ALL
SELECT *, 'day' AS TIMEFRAME FROM agg_data
)

, week_metrics AS (
WITH seg_data AS (
    WITH project_counts AS (
      SELECT
        date_trunc('week', BLOCK_DATE) as DATE,
        CHAIN,
        COALESCE(ap.NAME, a.TO_ADDRESS, 'Contract Deployment') as PROJECT,
        COUNT(DISTINCT FROM_ADDRESS) AS NUM_WALLETS,
        ROW_NUMBER() OVER (PARTITION BY DATE, CHAIN ORDER BY NUM_WALLETS DESC) as rank
      FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS a
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_AUTHORIZED_CONTRACTS l
        ON a.AUTHORIZED_CONTRACT = l.ADDRESS
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_APPS ap 
        ON ap.ADDRESS = a.TO_ADDRESS
      WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        AND BLOCK_TIME < date_trunc('week', CURRENT_DATE())
      GROUP BY 1, 2, 3
    )
    SELECT
      TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
      CHAIN,
      CASE 
        WHEN rank <= 10 THEN PROJECT
        ELSE 'other'
      END AS PROJECT,
      SUM(NUM_WALLETS) AS NUM_UNIQUE_SENDERS
    FROM project_counts
    GROUP BY 1, 2, 3
)

, agg_data AS (
    WITH project_counts AS (
      SELECT
        date_trunc('week', BLOCK_DATE) as DATE,
        COALESCE(ap.NAME, a.TO_ADDRESS, 'Contract Deployment') as PROJECT,
        COUNT(DISTINCT FROM_ADDRESS) AS NUM_WALLETS,
        ROW_NUMBER() OVER (PARTITION BY DATE ORDER BY NUM_WALLETS DESC) as rank
      FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS a
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_AUTHORIZED_CONTRACTS l
        ON a.AUTHORIZED_CONTRACT = l.ADDRESS
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_APPS ap 
        ON ap.ADDRESS = a.TO_ADDRESS
      WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        AND BLOCK_TIME < date_trunc('week', CURRENT_DATE())
      GROUP BY 1, 2
    )
    SELECT
      TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
      'all' AS CHAIN,
      CASE 
        WHEN rank <= 10 THEN PROJECT
        ELSE 'other'
      END AS PROJECT,
      SUM(NUM_WALLETS) AS NUM_UNIQUE_SENDERS
    FROM project_counts
    GROUP BY 1, 2, 3
)

SELECT *, 'week' AS TIMEFRAME FROM seg_data 
UNION ALL
SELECT *, 'week' AS TIMEFRAME FROM agg_data
)

, month_metrics AS (
WITH seg_data AS (
    WITH project_counts AS (
      SELECT
        date_trunc('month', BLOCK_DATE) as DATE,
        CHAIN,
        COALESCE(ap.NAME, a.TO_ADDRESS, 'Contract Deployment') as PROJECT,
        COUNT(DISTINCT FROM_ADDRESS) AS NUM_WALLETS,
        ROW_NUMBER() OVER (PARTITION BY DATE, CHAIN ORDER BY NUM_WALLETS DESC) as rank
      FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS a
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_AUTHORIZED_CONTRACTS l
        ON a.AUTHORIZED_CONTRACT = l.ADDRESS
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_APPS ap 
        ON ap.ADDRESS = a.TO_ADDRESS
      WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        AND BLOCK_TIME < date_trunc('month', CURRENT_DATE())
      GROUP BY 1, 2, 3
    )
    SELECT
      TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
      CHAIN,
      CASE 
        WHEN rank <= 10 THEN PROJECT
        ELSE 'other'
      END AS PROJECT,
      SUM(NUM_WALLETS) AS NUM_UNIQUE_SENDERS
    FROM project_counts
    GROUP BY 1, 2, 3
)

, agg_data AS (
    WITH project_counts AS (
      SELECT
        date_trunc('month', BLOCK_DATE) as DATE,
        COALESCE(ap.NAME, a.TO_ADDRESS, 'Contract Deployment') as PROJECT,
        COUNT(DISTINCT FROM_ADDRESS) AS NUM_WALLETS,
        ROW_NUMBER() OVER (PARTITION BY DATE ORDER BY NUM_WALLETS DESC) as rank
      FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS a
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_AUTHORIZED_CONTRACTS l
        ON a.AUTHORIZED_CONTRACT = l.ADDRESS
      LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_APPS ap 
        ON ap.ADDRESS = a.TO_ADDRESS
      WHERE BLOCK_TIME > DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '6 months'
        AND BLOCK_TIME < date_trunc('month', CURRENT_DATE())
      GROUP BY 1, 2
    )
    SELECT
      TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
      'all' AS CHAIN,
      CASE 
        WHEN rank <= 10 THEN PROJECT
        ELSE 'other'
      END AS PROJECT,
      SUM(NUM_WALLETS) AS NUM_UNIQUE_SENDERS
    FROM project_counts
    GROUP BY 1, 2, 3
)

SELECT *, 'month' AS TIMEFRAME FROM seg_data 
UNION ALL
SELECT *, 'month' AS TIMEFRAME FROM agg_data
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