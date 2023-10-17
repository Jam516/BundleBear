{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash','tx_hash']
)
}}

SELECT 
    BLOCK_TIMESTAMP as block_time,
    TRANSACTION_HASH as tx_hash,
    DECODED_INPUT:"opInfo":"userOpHash"::STRING as op_hash,
    DECODED_INPUT:"opInfo":"mUserOp"."sender"::STRING as sender,
    DECODED_INPUT:"opInfo":"mUserOp"."paymaster"::STRING as paymaster,
    DECODED_INPUT:"opInfo" as opInfo,
    TO_ADDRESS as contract_address,
    -- STATUS as call_success,
    TRACE_ADDRESS as call_trace_address,
    PARAMS as params,
    output,
    value
FROM {{ source('base_decoded', 'traces__beta') }}
WHERE TO_ADDRESS = '0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789'
    AND NAME = 'innerHandleOp'
    -- AND STATUS = 1
    {% if is_incremental() %}
    AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}