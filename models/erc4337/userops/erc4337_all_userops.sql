{{ config
(
    materialized = 'table'
)
}}

SELECT  *, 'arbitrum' AS chain FROM {{ ref('erc4337_arbitrum_userops') }}
UNION ALL
SELECT  *, 'avalanche' AS chain FROM {{ ref('erc4337_avalanche_userops') }}
UNION ALL
SELECT  *, 'base' AS chain FROM {{ ref('erc4337_base_userops') }}
UNION ALL
SELECT  *, 'ethereum' AS chain FROM {{ ref('erc4337_ethereum_userops') }}
UNION ALL
SELECT  *, 'optimism' AS chain FROM {{ ref('erc4337_optimism_userops') }}
UNION ALL
SELECT  *, 'polygon' AS chain FROM {{ ref('erc4337_polygon_userops') }}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('erc4337_bsc_userops') }}
