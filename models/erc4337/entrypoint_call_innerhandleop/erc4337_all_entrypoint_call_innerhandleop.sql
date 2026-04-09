{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['op_hash','tx_hash']
)
}}

SELECT  *, 'arbitrum' AS chain 
FROM {{ ref('erc4337_arbitrum_entrypoint_call_innerhandleop') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}

UNION ALL
SELECT  *, 'avalanche' AS chain 
FROM {{ ref('erc4337_avalanche_entrypoint_call_innerhandleop') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}

UNION ALL
SELECT  *, 'ethereum' AS chain 
FROM {{ ref('erc4337_ethereum_entrypoint_call_innerhandleop') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}

UNION ALL
SELECT  *, 'optimism' AS chain 
FROM {{ ref('erc4337_optimism_entrypoint_call_innerhandleop') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL

SELECT  *, 'polygon' AS chain 
FROM {{ ref('erc4337_polygon_entrypoint_call_innerhandleop') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}

UNION ALL
SELECT  *, 'bsc' AS chain 
FROM {{ ref('erc4337_bsc_entrypoint_call_innerhandleop') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}

UNION ALL
SELECT  *, 'base' AS chain 
FROM {{ ref('erc4337_base_entrypoint_call_innerhandleop') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
