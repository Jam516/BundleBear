{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

-- Per-chain metrics
WITH chain_metrics AS (
    SELECT 
        s.DAY,
        s.CHAIN,
        COALESCE(l.NAME, s.AUTHORIZED_CONTRACT) AS AUTHORIZED_CONTRACT,
        COUNT(DISTINCT s.AUTHORITY) AS NUM_WALLETS
    FROM {{ ref('eip7702_state_base') }} s
    LEFT JOIN {{ ref('eip7702_labels_authorized_contracts') }} l
        ON s.AUTHORIZED_CONTRACT = l.ADDRESS
    WHERE s.IS_SMART_WALLET = True
    GROUP BY 1, 2, 3
),

-- Cross-chain metrics (deduplicated across chains per day)
cross_chain_metrics AS (
    SELECT 
        s.DAY,
        'all' AS CHAIN,
        COALESCE(l.NAME, s.AUTHORIZED_CONTRACT) AS AUTHORIZED_CONTRACT,
        COUNT(DISTINCT s.AUTHORITY) AS NUM_WALLETS
    FROM {{ ref('eip7702_state_base') }} s
    LEFT JOIN {{ ref('eip7702_labels_authorized_contracts') }} l
        ON s.AUTHORIZED_CONTRACT = l.ADDRESS
    WHERE s.IS_SMART_WALLET = True
    GROUP BY 1, 3
),

combined AS (
    SELECT * FROM chain_metrics
    UNION ALL
    SELECT * FROM cross_chain_metrics
),

ranked_contracts AS (
    SELECT
        DAY,
        CHAIN,
        AUTHORIZED_CONTRACT,
        NUM_WALLETS,
        ROW_NUMBER() OVER (PARTITION BY DAY, CHAIN ORDER BY NUM_WALLETS DESC) AS rank
    FROM combined
)

SELECT
    TO_VARCHAR(DAY, 'YYYY-MM-DD') AS DATE,
    CHAIN,
    CASE WHEN rank <= 10 THEN AUTHORIZED_CONTRACT ELSE 'other' END AS AUTHORIZED_CONTRACT,
    SUM(NUM_WALLETS) AS NUM_WALLETS
FROM ranked_contracts
GROUP BY 1, 2, 3