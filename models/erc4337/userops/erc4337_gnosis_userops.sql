{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash', 'tx_hash']
)
}}

with op as (
    with base as (
        SELECT 
            op.sender
            , op.paymaster
            , op.op_hash
            , common.udfs.js_hextoint_secure(op.output)/1e18 as output_actualGasCost
            , op.tx_hash
            , op.block_time
            , op.call_trace_address
            , tx.FROM_ADDRESS as bundler
            , op.executeCall
        FROM {{ ref('erc4337_gnosis_entrypoint_call_innerhandleop') }} op
        INNER JOIN {{ source('gnosis_raw', 'transactions') }} tx 
            ON op.block_time = tx.BLOCK_TIMESTAMP AND op.tx_hash = tx.HASH
            {% if is_incremental() %}
            AND tx.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
            {% endif %}
            {% if not is_incremental() %}
            AND tx.BLOCK_TIMESTAMP >= to_timestamp('2023-04-01', 'yyyy-MM-dd') -- first gnosis entrypoint live
            {% endif %}
    )

    , joined as (
        SELECT
             b.*
             , t.TO_ADDRESS
             , t.INPUT
             , t.TRACE_ADDRESS
             , row_number() over (partition by b.sender, call_trace_address, tx_hash order by t.TRACE_ADDRESS asc) as first_call
        FROM base b
        INNER JOIN {{ source('gnosis_raw', 'traces') }} t 
            ON b.block_time = t.BLOCK_TIMESTAMP
            AND b.tx_hash = t.TRANSACTION_HASH
            AND b.sender = t.FROM_ADDRESS
            AND split(b.call_trace_address, ',')[0] = split(t.TRACE_ADDRESS, ',')[0] 
            -- AND _CREATED_AT IS NOT NULL
            {% if not is_incremental() %}
            AND t.BLOCK_TIMESTAMP >= to_timestamp('2023-04-01', 'yyyy-MM-dd') 
            {% endif %}
            {% if is_incremental() %}
            AND t.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
            {% endif %}            
            AND ARRAY_SIZE(split(t.TRACE_ADDRESS, ',')) > 3
            AND t.CALL_TYPE != 'delegatecall'
    )
    
    SELECT * FROM joined
    WHERE first_call = 1 
)

SELECT 
    block_time
    , tx_hash
    , op_hash
    , sender
    , bundler
    , COALESCE(b.name, 'Unknown') as bundler_name
    , paymaster
    , case when paymaster = '0x0000000000000000000000000000000000000000' then 'No paymaster'
      else COALESCE(pay.name, 'Unknown') 
      end as paymaster_name
    , case when paymaster = '0x0000000000000000000000000000000000000000' then 'No paymaster'
      else COALESCE(pay.type, 'Unknown') 
      end as paymaster_type
    , case 
        when INPUT != '0x' then TO_ADDRESS
        when (INPUT = '0x' AND common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64))/1e18 > 0) then 'direct_transfer'
        else 'empty_call' 
        end as called_contract
    , case 
        when INPUT != '0x' then COALESCE(TEXT_SIGNATURE_SHORT, LEFT(INPUT,10))
        when (INPUT = '0x' AND common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64))/1e18 > 0) then 'xdai_transfer'
        else 'empty_call' end as function_called
    , output_actualGasCost as actualgascost
    , output_actualGasCost * p.PRICE as actualgascost_usd
    , case when INPUT != '0x' then 0
      else common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64))/1e18 
      end as value
FROM op 
INNER JOIN {{ source('common_prices', 'hourly') }} p 
    ON p.TIMESTAMP = date_trunc('hour', block_time)
    AND p.ADDRESS = '0x0000000000000000000000000000000000000000' 
    AND p.CHAIN = 'gnosis'
    {% if is_incremental() %}
    AND p.TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %} 
LEFT JOIN {{ ref('erc4337_labels_bundlers') }} b ON b.address = op.bundler
LEFT JOIN {{ ref('erc4337_labels_paymasters') }} pay ON pay.address = op.paymaster
LEFT JOIN {{ source('common_decoded', 'function_signatures') }} s ON s.HEX_SIGNATURE = LEFT(INPUT,10)