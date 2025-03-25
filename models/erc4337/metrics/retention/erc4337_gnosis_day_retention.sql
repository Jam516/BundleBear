{{ config
(
    materialized = 'table'
)
}}

WITH transactions AS (
    SELECT SENDER, BLOCK_TIME AS created_at
    FROM {{ ref('erc4337_gnosis_userops') }}
),

cohort AS (
    SELECT 
    SENDER,
    MIN(date_trunc('day', created_at)) AS cohort_day
    FROM transactions
    GROUP BY 1
),

cohort_size AS (
    SELECT
    cohort_day,
    COUNT(1) as num_users
    FROM cohort
    GROUP BY cohort_day
),

user_activities AS (
    SELECT
    DISTINCT
        DATEDIFF(day, cohort_day, created_at) AS day_number,
        A.SENDER
    FROM transactions AS A
    LEFT JOIN cohort AS C 
    ON A.SENDER = C.SENDER
),

retention_table AS (
    SELECT
    cohort_day,
    A.day_number,
    COUNT(1) AS num_users
    FROM user_activities A
    LEFT JOIN cohort AS C 
    ON A.SENDER = C.SENDER
    GROUP BY 1, 2  
)

SELECT
    TO_VARCHAR(date_trunc('day', A.cohort_day), 'YYYY-MM-DD') AS cohort,
    B.num_users AS total_users,
    A.day_number,
    ROUND((A.num_users * 100 / B.num_users), 2) as percentage
FROM retention_table AS A
LEFT JOIN cohort_size AS B
ON A.cohort_day = B.cohort_day
WHERE 
    A.cohort_day IS NOT NULL
    AND A.cohort_day >= date_trunc('day', (CURRENT_TIMESTAMP() - interval '12 day'))  
    AND A.cohort_day < date_trunc('day', CURRENT_TIMESTAMP())
ORDER BY 1, 3