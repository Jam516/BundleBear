{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['op_hash','tx_hash']
)
}}

SELECT
block_time
, tx_hash
, op_hash
, sender
, 'arbitrum' AS chain
, ARRAY_AGG(DISTINCT called_contract) AS called_contracts
FROM {{ ref('erc4337_arbitrum_userops_unbatched') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
GROUP BY 1,2,3,4,5

UNION ALL
SELECT
block_time
, tx_hash
, op_hash
, sender
, 'avalanche' AS chain
, ARRAY_AGG(DISTINCT called_contract) AS called_contracts
FROM {{ ref('erc4337_avalanche_userops_unbatched') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
GROUP BY 1,2,3,4,5

UNION ALL
SELECT
block_time
, tx_hash
, op_hash
, sender
, 'ethereum' AS chain
, ARRAY_AGG(DISTINCT called_contract) AS called_contracts
FROM {{ ref('erc4337_ethereum_userops_unbatched') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
GROUP BY 1,2,3,4,5

UNION ALL
SELECT
block_time
, tx_hash
, op_hash
, sender
, 'optimism' AS chain
, ARRAY_AGG(DISTINCT called_contract) AS called_contracts
FROM {{ ref('erc4337_optimism_userops_unbatched') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
GROUP BY 1,2,3,4,5

UNION ALL
SELECT
block_time
, tx_hash
, op_hash
, sender
, 'polygon' AS chain
, ARRAY_AGG(DISTINCT called_contract) AS called_contracts
FROM {{ ref('erc4337_polygon_userops_unbatched') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
GROUP BY 1,2,3,4,5

UNION ALL
SELECT
block_time
, tx_hash
, op_hash
, sender
, 'bsc' AS chain 
, ARRAY_AGG(DISTINCT called_contract) AS called_contracts
FROM {{ ref('erc4337_bsc_userops_unbatched') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
GROUP BY 1,2,3,4,5

UNION ALL
SELECT 
block_time
, tx_hash
, op_hash
, sender
, 'base' AS chain 
, ARRAY_AGG(DISTINCT called_contract) AS called_contracts
FROM {{ ref('erc4337_base_userops_unbatched') }}
{% if is_incremental() %}
WHERE block_time::DATE >= CURRENT_TIMESTAMP() - interval '3 day'
{% endif %}
GROUP BY 1,2,3,4,5