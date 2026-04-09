{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['op_hash','tx_hash']
)
}}

SELECT  *, 'arbitrum' AS chain FROM {{ ref('erc4337_arbitrum_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'avalanche' AS chain FROM {{ ref('erc4337_avalanche_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'ethereum' AS chain FROM {{ ref('erc4337_ethereum_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'optimism' AS chain FROM {{ ref('erc4337_optimism_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'polygon' AS chain FROM {{ ref('erc4337_polygon_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('erc4337_bsc_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'linea' AS chain FROM {{ ref('erc4337_linea_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'celo' AS chain FROM {{ ref('erc4337_celo_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'arbitrum_nova' AS chain FROM {{ ref('erc4337_arbitrum_nova_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'gnosis' AS chain FROM {{ ref('erc4337_gnosis_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT  *, 'worldchain' AS chain FROM {{ ref('erc4337_worldchain_userops') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
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
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
