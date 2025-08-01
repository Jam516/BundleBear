{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT  *, 'base' AS chain FROM {{ ref('eip7702_base_authorizations') }}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('eip7702_bsc_authorizations') }}
UNION ALL
SELECT  *, 'ethereum' AS chain FROM {{ ref('eip7702_ethereum_authorizations') }}
UNION ALL
SELECT  *, 'gnosis' AS chain FROM {{ ref('eip7702_gnosis_authorizations') }}
UNION ALL
SELECT  *, 'optimism' AS chain FROM {{ ref('eip7702_optimism_authorizations') }}
UNION ALL
SELECT  *, 'arbitrum' AS chain FROM {{ ref('eip7702_arbitrum_authorizations') }}
UNION ALL
SELECT  *, 'unichain' AS chain FROM {{ ref('eip7702_unichain_authorizations') }}
UNION ALL
SELECT  *, 'polygon' AS chain FROM {{ ref('eip7702_polygon_authorizations') }}