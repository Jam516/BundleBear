{{ config
(
    materialized = 'incremental',
    unique_key = 'call_tx_hash'
)
}}

SELECT 
    BLOCK_NUMBER AS call_block_number,
    BLOCK_TIMESTAMP AS call_block_time,
    TRANSACTION_HASH AS call_tx_hash,
    PARAMS:"callData"::STRING AS callData,
    PARAMS:"opInfo" AS opInfo,
    TO_ADDRESS AS contract_address,
    STATUS AS call_success,
    TRACE_ADDRESS AS call_trace_address,
    PARAMS AS params,
    "OUTPUT"
FROM {{ source('ethereum_decoded', 'traces') }}
WHERE TO_ADDRESS IN 
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0576a174d229e3cfa37253523e645a78a0c91b57', 
    '0x0f46c65c17aa6b4102046935f33301f0510b163a')
    AND NAME = 'innerHandleOp'
    {% if is_incremental() %}
    AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '1 day' 
    {% endif %}