{{ config
(
    materialized = 'incremental',
    partition_by = ['block_date'],
    unique_key = ['block_date','tx_hash', 'nonce', 'authorized_contract', 'authority']
)
}}

SELECT DISTINCT
    t.BLOCK_TIMESTAMP AS block_time,
    date_trunc('day', t.BLOCK_TIMESTAMP) AS block_date,
    date_trunc('week', t.BLOCK_TIMESTAMP) AS block_week,
    date_trunc('month', t.BLOCK_TIMESTAMP) AS block_month,
    t.HASH AS tx_hash,
    COMMON.UDFS.JS_HEXTOINT_SECURE(REPLACE(a.value:chainId::STRING, '0x', '')) AS chain_id,
    COMMON.UDFS.JS_HEXTOINT_SECURE(REPLACE(a.value:nonce::STRING, '0x', '')) AS nonce,
    a.value:address::STRING AS authorized_contract,
    a.value:authority::STRING AS authority
FROM 
    {{ source('base_raw', 'transactions') }} t,
    LATERAL FLATTEN(input => t.AUTHORIZATION_LIST) a
WHERE 
    t.TRANSACTION_TYPE =4
    AND ARRAY_SIZE(t.AUTHORIZATION_LIST) > 0
    {% if is_incremental() %}
    AND t.BLOCK_TIMESTAMP >= CURRENT_DATE() - interval '3 day' 
    {% endif %}