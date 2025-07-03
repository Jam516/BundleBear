{{ config
(
    materialized = 'table',
    copy_grants=true,
    cluster_by = ['block_date', 'chain']
)
}}

SELECT  * FROM {{ ref('eip7702_base_actions') }}
UNION ALL
SELECT  * FROM {{ ref('eip7702_bsc_actions') }}
UNION ALL
SELECT  * FROM {{ ref('eip7702_ethereum_actions') }}
UNION ALL
SELECT  * FROM {{ ref('eip7702_gnosis_actions') }}
UNION ALL
SELECT  * FROM {{ ref('eip7702_optimism_actions') }}
UNION ALL
SELECT  * FROM {{ ref('eip7702_arbitrum_actions') }}
UNION ALL
SELECT  * FROM {{ ref('eip7702_unichain_actions') }}
UNION ALL
SELECT  * FROM {{ ref('eip7702_polygon_actions') }}