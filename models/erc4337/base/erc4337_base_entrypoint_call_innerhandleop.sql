{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash','tx_hash']
)
}}

SELECT 
    dt.BLOCK_TIMESTAMP as block_time,
    dt.TRANSACTION_HASH as tx_hash,
    dt.DECODED_INPUT:"input_params":"opInfo":"userOpHash"::STRING as op_hash,
    dt.DECODED_INPUT:"input_params":"opInfo":"mUserOp"."sender"::STRING as sender,
    dt.DECODED_INPUT:"input_params":"opInfo":"mUserOp"."paymaster"::STRING as paymaster,
    dt.DECODED_INPUT:"input_params":"callData" as executeCall,
    dt.TO_ADDRESS as contract_address,
    rt.STATUS as call_success,
    rt.TRACE_ADDRESS as call_trace_address,
    dt.DECODED_INPUT as params,
    rt.output
FROM {{ source('base_decoded', 'traces__beta') }} dt
INNER JOIN {{ source('base_raw', 'traces') }} rt
    ON dt.TRACE_ID = rt.TRACE_ID
    AND dt.TO_ADDRESS = '0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789'
    AND dt.SELECTOR = '0x1d732756' -- innerhandleop
    AND rt.ERROR IS NULL
    {% if is_incremental() %}
    AND dt.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}