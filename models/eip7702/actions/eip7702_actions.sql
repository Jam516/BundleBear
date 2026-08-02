{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    cluster_by = ['block_date', 'chain'],
    unique_key = ['block_time', 'chain', 'tx_hash', 'op_hash']
)
}}

SELECT * FROM {{ ref('eip7702_base_actions') }}
{% if is_incremental() %}
WHERE block_time >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT * FROM {{ ref('eip7702_bsc_actions') }}
{% if is_incremental() %}
WHERE block_time >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT * FROM {{ ref('eip7702_ethereum_actions') }}
{% if is_incremental() %}
WHERE block_time >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT * FROM {{ ref('eip7702_gnosis_actions') }}
{% if is_incremental() %}
WHERE block_time >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT * FROM {{ ref('eip7702_optimism_actions') }}
{% if is_incremental() %}
WHERE block_time >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT * FROM {{ ref('eip7702_arbitrum_actions') }}
{% if is_incremental() %}
WHERE block_time >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT * FROM {{ ref('eip7702_unichain_actions') }}
{% if is_incremental() %}
WHERE block_time >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
UNION ALL
SELECT * FROM {{ ref('eip7702_polygon_actions') }}
{% if is_incremental() %}
WHERE block_time >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}