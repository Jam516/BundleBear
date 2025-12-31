{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['day', 'chain']
)
}}

WITH chain_metrics AS (
SELECT
  day,
  CHAIN,
  COUNT(DISTINCT CASE WHEN is_smart_wallet THEN AUTHORITY END) AS live_smart_wallets,
  COUNT(DISTINCT CASE WHEN is_smart_wallet THEN AUTHORIZED_CONTRACT END) AS live_authorized_contracts
FROM 
  {{ ref('eip7702_state_base') }}
{% if is_incremental() %}
WHERE day >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
GROUP BY 1, 2
),

cross_chain_metrics AS (
  SELECT 
    day,
    'cross-chain' AS CHAIN,
    COUNT(DISTINCT CASE WHEN is_smart_wallet THEN AUTHORITY END) AS live_smart_wallets,
  COUNT(DISTINCT CASE WHEN is_smart_wallet THEN AUTHORIZED_CONTRACT END) AS live_authorized_contracts
FROM 
  {{ ref('eip7702_state_base') }}
{% if is_incremental() %}
WHERE day >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
GROUP BY 1, 2
)

SELECT * FROM chain_metrics
UNION ALL
SELECT * FROM cross_chain_metrics