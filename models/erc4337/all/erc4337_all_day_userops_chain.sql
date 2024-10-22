{{ config
(
    materialized = 'table'
)
}}

SELECT 
DATE,
CHAIN,
SUM(NUM_USEROPS) AS NUM_USEROPS
FROM 
(
    SELECT 'arbitrum' AS CHAIN, * FROM {{ ref('erc4337_arbitrum_day_userops_chain') }}
    UNION ALL 
    SELECT 'avalanche' AS CHAIN, * FROM {{ ref('erc4337_avalanche_day_userops_chain') }}
    UNION ALL 
    SELECT 'base' AS CHAIN, * FROM {{ ref('erc4337_base_day_userops_chain') }}
    UNION ALL 
    SELECT 'ethereum' AS CHAIN, * FROM {{ ref('erc4337_ethereum_day_userops_chain') }}
    UNION ALL 
    SELECT 'optimism' AS CHAIN, * FROM {{ ref('erc4337_optimism_day_userops_chain') }}
    UNION ALL 
    SELECT 'polygon' AS CHAIN, * FROM {{ ref('erc4337_polygon_day_userops_chain') }}
)
GROUP BY 1,2