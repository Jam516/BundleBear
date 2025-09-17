{{ config
(
    materialized = 'incremental',
    partition_by = ['block_date'],
    unique_key = ['block_date','tx_hash', 'nonce', 'authorized_contract', 'authority','is_valid']
)
}}

SELECT DISTINCT
    t.BLOCK_TIMESTAMP AS block_time,
    date_trunc('day', t.BLOCK_TIMESTAMP) AS block_date,
    date_trunc('week', t.BLOCK_TIMESTAMP) AS block_week,
    date_trunc('month', t.BLOCK_TIMESTAMP) AS block_month,
    t.HASH AS tx_hash,
    a.value:chain_id::INTEGER AS chain_id,
    a.value:nonce::INTEGER AS nonce,
    a.value:address::STRING AS authorized_contract,
    a.value:authority::STRING AS authority,
    a.value:is_valid::BOOLEAN AS is_valid
FROM 
    {{ source('ethereum_raw', 'transactions') }} t,
    LATERAL FLATTEN(input => t.AUTHORIZATION_LIST) a
WHERE 
    t.TRANSACTION_TYPE =4
    AND ARRAY_SIZE(t.AUTHORIZATION_LIST) > 0
    {% if is_incremental() %}
    AND t.BLOCK_TIMESTAMP >= CURRENT_DATE() - interval '3 day' 
    {% endif %}