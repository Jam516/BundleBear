{{ config
(
    materialized = 'table'
)
}}

SELECT  *, 'arbitrum' AS chain FROM {{ ref('erc4337_arbitrum_failed_validation_ops') }}
UNION ALL
SELECT  *, 'avalanche' AS chain FROM {{ ref('erc4337_avalanche_failed_validation_ops') }}
UNION ALL
SELECT  *, 'base' AS chain FROM {{ ref('erc4337_base_failed_validation_ops') }}
UNION ALL
SELECT  *, 'ethereum' AS chain FROM {{ ref('erc4337_ethereum_failed_validation_ops') }}
UNION ALL
SELECT  *, 'optimism' AS chain FROM {{ ref('erc4337_optimism_failed_validation_ops') }}
UNION ALL
SELECT  *, 'polygon' AS chain FROM {{ ref('erc4337_polygon_failed_validation_ops') }}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('erc4337_bsc_failed_validation_ops') }}
UNION ALL
SELECT  *, 'linea' AS chain FROM {{ ref('erc4337_linea_failed_validation_ops') }}
UNION ALL
SELECT  *, 'celo' AS chain FROM {{ ref('erc4337_celo_failed_validation_ops') }}