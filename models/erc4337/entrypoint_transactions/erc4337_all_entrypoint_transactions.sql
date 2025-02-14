{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT  *, 'arbitrum' AS chain FROM {{ ref('erc4337_arbitrum_entrypoint_transactions') }}
UNION ALL
SELECT  *, 'avalanche' AS chain FROM {{ ref('erc4337_avalanche_entrypoint_transactions') }}
UNION ALL
SELECT  *, 'base' AS chain FROM {{ ref('erc4337_base_entrypoint_transactions') }}
UNION ALL
SELECT  *, 'ethereum' AS chain FROM {{ ref('erc4337_ethereum_entrypoint_transactions') }}
UNION ALL
SELECT  *, 'optimism' AS chain FROM {{ ref('erc4337_optimism_entrypoint_transactions') }}
UNION ALL
SELECT  *, 'polygon' AS chain FROM {{ ref('erc4337_polygon_entrypoint_transactions') }}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('erc4337_bsc_entrypoint_transactions') }}
UNION ALL
SELECT  *, 'linea' AS chain FROM {{ ref('erc4337_linea_entrypoint_transactions') }}
UNION ALL
SELECT  *, 'celo' AS chain FROM {{ ref('erc4337_celo_entrypoint_transactions') }}
UNION ALL
SELECT  *, 'arbitrum_nova' AS chain FROM {{ ref('erc4337_arbitrum_nova_entrypoint_transactions') }}