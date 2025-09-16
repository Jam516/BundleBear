{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

WITH latest_date AS (
    SELECT MAX(day) AS max_date 
    FROM bundlebear.dbt_kofi.eip7702_metrics_daily_authority_state
),

authorizations_summary AS (
    SELECT 
        chain,
        COUNT(*) AS num_authorizations,
        COUNT(DISTINCT tx_hash) AS num_set_code_txns
    FROM bundlebear.dbt_kofi.eip7702_all_authorizations
    WHERE IS_VALID = True
    GROUP BY chain
),

state_summary AS (
    SELECT 
        chain,
        live_smart_wallets
    FROM bundlebear.dbt_kofi.eip7702_metrics_daily_authority_state
    WHERE day = (SELECT max_date FROM latest_date)
)

-- Cross-chain aggregate
SELECT 
    'all' AS chain,
    s.live_smart_wallets,
    SUM(a.num_authorizations) AS num_authorizations,
    SUM(a.num_set_code_txns) AS num_set_code_txns
FROM authorizations_summary a
CROSS JOIN state_summary s
WHERE s.chain = 'cross-chain'
GROUP BY s.live_smart_wallets

UNION ALL

-- Per-chain detail
SELECT 
    a.chain,
    s.live_smart_wallets,
    a.num_authorizations,
    a.num_set_code_txns
FROM authorizations_summary a
INNER JOIN state_summary s
    ON a.chain = s.chain
WHERE s.chain != 'cross-chain'