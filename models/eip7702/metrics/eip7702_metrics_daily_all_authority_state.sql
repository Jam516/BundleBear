{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT
day,
chain,
live_smart_wallets,
live_authorized_contracts
FROM {{ ref('eip7702_metrics_daily_single_chain_authority_state') }} 

UNION ALL
SELECT
day,
chain,
live_smart_wallets,
live_authorized_contracts
FROM {{ ref('eip7702_metrics_daily_cross_chain_authority_state') }}