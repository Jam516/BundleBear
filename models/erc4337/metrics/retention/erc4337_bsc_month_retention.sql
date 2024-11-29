{{ config
(
    materialized = 'table'
)
}}

WITH transactions AS (
    SELECT SENDER, BLOCK_TIME AS created_at
    FROM {{ ref('erc4337_bsc_userops') }}
),

cohort AS (
    SELECT 
    SENDER,
    MIN(date_trunc('month', created_at)) AS cohort_month
    FROM transactions
    GROUP BY 1
),

cohort_size AS (
    SELECT
    cohort_month,
    COUNT(1) as num_users
    FROM cohort
    GROUP BY cohort_month
),

user_activities AS (
    SELECT
    DISTINCT
        DATEDIFF(month, cohort_month, created_at) AS month_number,
        A.SENDER
    FROM transactions AS A
    LEFT JOIN cohort AS C 
    ON A.SENDER = C.SENDER
),

retention_table AS (
    SELECT
    cohort_month,
    A.month_number,
    COUNT(1) AS num_users
    FROM user_activities A
    LEFT JOIN cohort AS C 
    ON A.SENDER = C.SENDER
    GROUP BY 1, 2  
)

SELECT
    TO_VARCHAR(date_trunc('month', A.cohort_month), 'YYYY-MM-DD') AS cohort,
    B.num_users AS total_users,
    A.month_number,
    ROUND((A.num_users * 100 / B.num_users), 2) as percentage
FROM retention_table AS A
LEFT JOIN cohort_size AS B
ON A.cohort_month = B.cohort_month
WHERE 
    A.cohort_month IS NOT NULL
    AND A.cohort_month >= date_trunc('month', (CURRENT_TIMESTAMP() - interval '12 month'))  
    AND A.cohort_month < date_trunc('month', CURRENT_TIMESTAMP())
ORDER BY 1, 3