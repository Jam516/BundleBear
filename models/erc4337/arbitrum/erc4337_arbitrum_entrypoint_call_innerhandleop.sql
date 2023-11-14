{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash','tx_hash']
)
}}

SELECT 
    BLOCK_TIMESTAMP as block_time,
    TRANSACTION_HASH as tx_hash,
    PARAMS:"opInfo":"userOpHash"::STRING as op_hash,
    PARAMS:"opInfo":"mUserOp"."sender"::STRING as sender,
    PARAMS:"opInfo":"mUserOp"."paymaster"::STRING as paymaster,
    PARAMS:"calldata" as executeCall,
    TO_ADDRESS as contract_address,
    STATUS as call_success,
    TRACE_ADDRESS as call_trace_address,
    PARAMS as params,
    output
FROM {{ source('arbitrum_decoded', 'traces_sample') }}
WHERE TO_ADDRESS IN 
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0576a174d229e3cfa37253523e645a78a0c91b57', 
    '0x0f46c65c17aa6b4102046935f33301f0510b163a')
    AND NAME = 'innerHandleOp'
    AND STATUS = 1
    {% if is_incremental() %}
    AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}