{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash','tx_hash']
)
}}

SELECT 
    BLOCK_TIMESTAMP as block_time,
    TRANSACTION_HASH as tx_hash,
    INPUT_PARAMS:"opInfo":"userOpHash"::STRING as op_hash,
    INPUT_PARAMS:"opInfo":"mUserOp"."sender"::STRING as sender,
    INPUT_PARAMS:"opInfo":"mUserOp"."paymaster"::STRING as paymaster,
    INPUT_PARAMS:"callData"::STRING as executeCall,
    TO_ADDRESS as contract_address,
    STATUS as call_success,
    TRACE_ADDRESS as call_trace_address,
    INPUT_PARAMS as params,
    output
FROM {{ source('ethereum_decoded', 'traces') }}
WHERE TO_ADDRESS IN 
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0576a174d229e3cfa37253523e645a78a0c91b57', 
    '0x0f46c65c17aa6b4102046935f33301f0510b163a',
    '0x0000000071727de22e5e9d8baf0edac6f37da032',
    '0x4337084d9e255ff0702461cf8895ce9e3b5ff108')
    AND NAME = 'innerHandleOp'
    AND ERROR IS NULL
    {% if is_incremental() %}
    AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}