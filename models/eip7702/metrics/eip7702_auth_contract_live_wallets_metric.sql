{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'chain', 'authorized_contract']
)
}}
-- Per-chain metrics
WITH date_spine AS (
{% if is_incremental() %}
    SELECT DATEADD(DAY, -1 - SEQ4(), CURRENT_DATE()) AS day
    FROM TABLE(GENERATOR(ROWCOUNT => 3))
{% else %}
    SELECT DATEADD(DAY, seq, DATE('2025-03-20')) AS day
    FROM (
        SELECT SEQ4() AS seq
        FROM TABLE(GENERATOR(ROWCOUNT => 2000))
    )
    WHERE seq <= DATEDIFF('DAY', DATE('2025-03-20'), DATEADD(DAY, -1, CURRENT_DATE()))
{% endif %}
),

chain_metrics AS (
    SELECT
        d.DAY,
        s.CHAIN,
        COALESCE(l.NAME, s.AUTHORIZED_CONTRACT) AS AUTHORIZED_CONTRACT,
        COUNT(DISTINCT s.AUTHORITY) AS NUM_WALLETS
    FROM date_spine d
    INNER JOIN {{ ref('eip7702_state_base') }} s
        ON d.day >= s.start_day
        AND (s.end_day IS NULL OR d.day <= s.end_day)
    LEFT JOIN {{ ref('eip7702_labels_authorized_contracts') }} l
        ON s.AUTHORIZED_CONTRACT = l.ADDRESS
    WHERE s.IS_SMART_WALLET = True
    GROUP BY 1, 2, 3
),

-- Cross-chain metrics (deduplicated across chains per day)
cross_chain_metrics AS (
    SELECT
        d.DAY,
        'all' AS CHAIN,
        COALESCE(l.NAME, s.AUTHORIZED_CONTRACT) AS AUTHORIZED_CONTRACT,
        COUNT(DISTINCT s.AUTHORITY) AS NUM_WALLETS
    FROM date_spine d
    INNER JOIN {{ ref('eip7702_state_base') }} s
        ON d.day >= s.start_day
        AND (s.end_day IS NULL OR d.day <= s.end_day)
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
