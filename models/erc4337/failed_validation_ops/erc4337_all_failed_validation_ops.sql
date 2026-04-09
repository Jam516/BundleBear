{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['TRACE_ID','TX_HASH']
)
}}

SELECT  *, 'arbitrum' AS chain FROM {{ ref('erc4337_arbitrum_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'avalanche' AS chain FROM {{ ref('erc4337_avalanche_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'base' AS chain FROM {{ ref('erc4337_base_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'ethereum' AS chain FROM {{ ref('erc4337_ethereum_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'optimism' AS chain FROM {{ ref('erc4337_optimism_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'polygon' AS chain FROM {{ ref('erc4337_polygon_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('erc4337_bsc_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'linea' AS chain FROM {{ ref('erc4337_linea_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'celo' AS chain FROM {{ ref('erc4337_celo_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'arbitrum_nova' AS chain FROM {{ ref('erc4337_arbitrum_nova_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'gnosis' AS chain FROM {{ ref('erc4337_gnosis_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'worldchain' AS chain FROM {{ ref('erc4337_worldchain_failed_validation_ops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}