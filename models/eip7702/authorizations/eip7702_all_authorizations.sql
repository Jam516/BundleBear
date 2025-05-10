{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT  *, 'base' AS chain FROM {{ ref('erc7702_base_authorizations') }}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('erc7702_bsc_authorizations') }}
UNION ALL
SELECT  *, 'ethereum' AS chain FROM {{ ref('erc7702_ethereum_authorizations') }}
UNION ALL
SELECT  *, 'gnosis' AS chain FROM {{ ref('erc7702_gnosis_authorizations') }}
UNION ALL
SELECT  *, 'optimism' AS chain FROM {{ ref('erc7702_optimism_authorizations') }}
UNION ALL
SELECT  *, 'worldchain' AS chain FROM {{ ref('erc7702_worldchain_authorizations') }}