{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}
WITH as_of_day AS (
    SELECT DATEADD(DAY, -1, CURRENT_DATE()) AS day
),

-- Per-chain metrics
chain_metrics AS (
    SELECT 
        s.CHAIN,
        COALESCE(l.NAME, s.AUTHORIZED_CONTRACT) AS AUTHORIZED_CONTRACT,
        COUNT(DISTINCT s.AUTHORITY) AS NUM_WALLETS
    FROM {{ ref('eip7702_state_base') }} s
    LEFT JOIN {{ ref('eip7702_labels_authorized_contracts') }} l
        ON s.AUTHORIZED_CONTRACT = l.ADDRESS
    CROSS JOIN as_of_day ad
    WHERE ad.day >= s.start_day
      AND (s.end_day IS NULL OR ad.day <= s.end_day)
      AND s.IS_SMART_WALLET = True
    GROUP BY 1, 2
),

-- Cross-chain metrics (deduplicated across chains)
cross_chain_metrics AS (
    SELECT
        'all' AS CHAIN,
        COALESCE(l.NAME, s.AUTHORIZED_CONTRACT) AS AUTHORIZED_CONTRACT,
        COUNT(DISTINCT s.AUTHORITY) AS NUM_WALLETS
    FROM {{ ref('eip7702_state_base') }} s
    LEFT JOIN {{ ref('eip7702_labels_authorized_contracts') }} l
        ON s.AUTHORIZED_CONTRACT = l.ADDRESS
    CROSS JOIN as_of_day ad
    WHERE ad.day >= s.start_day
      AND (s.end_day IS NULL OR ad.day <= s.end_day)
      AND s.IS_SMART_WALLET = True
    GROUP BY 2
),

combined AS (
    SELECT * FROM chain_metrics
    UNION ALL
    SELECT * FROM cross_chain_metrics
),

ranked AS (
    SELECT 
        CHAIN,
        AUTHORIZED_CONTRACT,
        NUM_WALLETS,
        ROW_NUMBER() OVER (PARTITION BY CHAIN ORDER BY NUM_WALLETS DESC) AS rn
    FROM combined
)

SELECT CHAIN, AUTHORIZED_CONTRACT, NUM_WALLETS
FROM ranked
WHERE rn <= 15
