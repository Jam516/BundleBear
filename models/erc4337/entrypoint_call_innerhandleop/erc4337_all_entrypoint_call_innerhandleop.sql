{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT  *, 'arbitrum' AS chain FROM {{ ref('erc4337_arbitrum_entrypoint_call_innerhandleop') }}
UNION ALL
SELECT  *, 'avalanche' AS chain FROM {{ ref('erc4337_avalanche_entrypoint_call_innerhandleop') }}
UNION ALL
SELECT  *, 'ethereum' AS chain FROM {{ ref('erc4337_ethereum_entrypoint_call_innerhandleop') }}
UNION ALL
SELECT  *, 'optimism' AS chain FROM {{ ref('erc4337_optimism_entrypoint_call_innerhandleop') }}
UNION ALL
SELECT  *, 'polygon' AS chain FROM {{ ref('erc4337_polygon_entrypoint_call_innerhandleop') }}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('erc4337_bsc_entrypoint_call_innerhandleop') }}
UNION ALL
SELECT  *, 'base' AS chain FROM {{ ref('erc4337_base_entrypoint_call_innerhandleop') }}
