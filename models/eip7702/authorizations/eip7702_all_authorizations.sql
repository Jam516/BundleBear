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
SELECT  *, 'worldchain' AS chain FROM {{ ref('eip7702_worldchain_authorizations') }}
