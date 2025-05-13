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

-- confirm that a chain_id 0 auth still needs a sect code on every chain before replacing this with the full single chain logic and deleting single chain table