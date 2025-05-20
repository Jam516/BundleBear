{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

WITH chain_metrics AS (
  SELECT 
    day,
    CHAIN,
    AUTHORIZED_CONTRACT,
    COUNT(DISTINCT AUTHORITY) AS NUM_WALLETS
  FROM 
    {{ ref('eip7702_state_base') }}
  WHERE 
    is_smart_wallet
  GROUP BY 
    day, CHAIN, AUTHORIZED_CONTRACT
),

cross_chain_metrics AS (
  SELECT 
    day,
    'cross-chain' AS CHAIN,
    AUTHORIZED_CONTRACT,
    COUNT(DISTINCT AUTHORITY) AS NUM_WALLETS
  FROM 
    {{ ref('eip7702_state_base') }}
  WHERE 
    is_smart_wallet
  GROUP BY 
    day, AUTHORIZED_CONTRACT
)

SELECT * FROM chain_metrics
UNION ALL
SELECT * FROM cross_chain_metrics