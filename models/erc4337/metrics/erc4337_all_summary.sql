{{ config
(
    materialized = 'table'
)
}}

SELECT 
SUM(NUM_DEPLOYMENTS) AS NUM_DEPLOYMENTS,
SUM(NUM_USEROPS) AS NUM_USEROPS,
SUM(NUM_TXNS) AS NUM_TXNS,
SUM(GAS_SPENT) AS GAS_SPENT
FROM 
(
    SELECT * FROM {{ ref('erc4337_arbitrum_summary') }}
    UNION ALL 
    SELECT * FROM {{ ref('erc4337_avalanche_summary') }}
    UNION ALL 
    SELECT * FROM {{ ref('erc4337_base_summary') }}
    UNION ALL 
    SELECT * FROM {{ ref('erc4337_ethereum_summary') }}
    UNION ALL 
    SELECT * FROM {{ ref('erc4337_optimism_summary') }}
    UNION ALL 
    SELECT * FROM {{ ref('erc4337_polygon_summary') }}
)