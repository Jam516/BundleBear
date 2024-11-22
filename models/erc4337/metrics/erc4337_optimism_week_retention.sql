{{ config
(
    materialized = 'table'
)
}}

WITH transactions AS (
    SELECT SENDER, BLOCK_TIME AS created_at
    FROM {{ ref('erc4337_optimism_userops') }}
),

cohort AS (
    SELECT 
    SENDER,
    MIN(date_trunc('week', created_at)) AS cohort_week
    FROM transactions
    GROUP BY 1
),

cohort_size AS (
    SELECT
    cohort_week,
    COUNT(1) as num_users
    FROM cohort
    GROUP BY cohort_week
),

user_activities AS (
    SELECT
    DISTINCT
        DATEDIFF(week, cohort_week, created_at) AS week_number,
        A.SENDER
    FROM transactions AS A
    LEFT JOIN cohort AS C 
    ON A.SENDER = C.SENDER
),

retention_table AS (
    SELECT
    cohort_week,
    A.week_number,
    COUNT(1) AS num_users
    FROM user_activities A
    LEFT JOIN cohort AS C 
    ON A.SENDER = C.SENDER
    GROUP BY 1, 2  
)

SELECT
    TO_VARCHAR(date_trunc('week', A.cohort_week), 'YYYY-MM-DD') AS cohort,
    B.num_users AS total_users,
    A.week_number,
    ROUND((A.num_users * 100 / B.num_users), 2) as percentage
FROM retention_table AS A
LEFT JOIN cohort_size AS B
ON A.cohort_week = B.cohort_week
WHERE 
    A.cohort_week IS NOT NULL
    AND A.cohort_week >= date_trunc('week', (CURRENT_TIMESTAMP() - interval '12 week'))  
    AND A.cohort_week < date_trunc('week', CURRENT_TIMESTAMP())
ORDER BY 1, 3