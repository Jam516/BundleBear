{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT  *, 'arbitrum' AS chain FROM {{ ref('erc4337_arbitrum_userops') }}
UNION ALL
SELECT  *, 'avalanche' AS chain FROM {{ ref('erc4337_avalanche_userops') }}
UNION ALL
SELECT  *, 'ethereum' AS chain FROM {{ ref('erc4337_ethereum_userops') }}
UNION ALL
SELECT  *, 'optimism' AS chain FROM {{ ref('erc4337_optimism_userops') }}
UNION ALL
SELECT  *, 'polygon' AS chain FROM {{ ref('erc4337_polygon_userops') }}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('erc4337_bsc_userops') }}
UNION ALL
SELECT  *, 'linea' AS chain FROM {{ ref('erc4337_linea_userops') }}
UNION ALL
SELECT  *, 'celo' AS chain FROM {{ ref('erc4337_celo_userops') }}
UNION ALL
SELECT  *, 'arbitrum_nova' AS chain FROM {{ ref('erc4337_arbitrum_nova_userops') }}
UNION ALL
SELECT  *, 'gnosis' AS chain FROM {{ ref('erc4337_gnosis_userops') }}
UNION ALL
SELECT  *, 'worldchain' AS chain FROM {{ ref('erc4337_worldchain_userops') }}
UNION ALL
SELECT 
block_time
, tx_hash
, op_hash
, sender
, bundler
, bundler_name
, paymaster
, paymaster_name
, paymaster_type
, called_contract
, function_called
, actualgascost
, actualgascost_usd
, value
, 'base' AS chain 
FROM {{ ref('erc4337_base_userops') }}
