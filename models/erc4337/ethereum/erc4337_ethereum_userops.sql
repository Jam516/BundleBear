{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash', 'bundle_tx_hash']
)
}}

with op as (
    with base as (
        SELECT 
            OPINFO:"mUserOp"."sender"::STRING as sender
            , OPINFO:"mUserOp"."paymaster"::STRING as paymaster
            , op_hash
            , common.udfs.js_hextoint_secure(output)/1e18 as output_actualGasCost
            , call_tx_hash
            , call_block_time
            , call_trace_address
            , call_block_number
            , tx.FROM_ADDRESS as bundler
        FROM {{ ref('erc4337_ethereum_entrypoint_call_innerhandleop') }} op
        INNER JOIN {{ source('ethereum_raw', 'transactions') }} tx 
            ON op.call_block_time = tx.BLOCK_TIMESTAMP AND op.call_tx_hash = tx.HASH
            {% if is_incremental() %}
            AND tx.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '1 day' 
            {% endif %}
        {% if not is_incremental() %}
        WHERE tx.BLOCK_TIMESTAMP >= to_timestamp('2023-01-27', 'yyyy-MM-dd') -- first mainnet entrypoint live
        {% endif %}
        {% if is_incremental() %}
        WHERE op.call_block_time >= CURRENT_TIMESTAMP() - interval '1 day' 
        {% endif %}
    )

    , joined as (
        SELECT
             b.*
             , t.TO_ADDRESS
             , t.INPUT
             , t.TRACE_ADDRESS
             , row_number() over (partition by b.sender, call_trace_address, call_tx_hash order by t.TRACE_ADDRESS asc) as first_call
        FROM base b
        INNER JOIN {{ source('ethereum_raw', 'traces') }} t 
            ON b.call_block_time = t.BLOCK_TIMESTAMP
            AND b.call_tx_hash = t.TRANSACTION_HASH
            AND b.sender = t.FROM_ADDRESS
            AND split(b.call_trace_address, ',')[0] = split(t.TRACE_ADDRESS, ',')[0] 
            {% if not is_incremental() %}
            AND t.BLOCK_TIMESTAMP >= to_timestamp('2023-01-27', 'yyyy-MM-dd') 
            {% endif %}
            {% if is_incremental() %}
            AND t.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '1 day' 
            {% endif %}            
        WHERE ARRAY_SIZE(split(t.TRACE_ADDRESS, ',')) > 3
            AND t.CALL_TYPE != 'delegatecall'
    )
    
    SELECT * FROM joined
    WHERE first_call = 1 
)

SELECT 
    call_tx_hash as bundle_tx_hash
    , op_hash
    , call_block_time as block_time
    , 'ethereum' as chain
    , sender
    , bundler
    , COALESCE(b.name, 'Unknown') as bundler_name
    , paymaster
    , COALESCE(pay.name, 'Unknown') as paymaster_name
    , case when INPUT != '0x0000000000000000000000000000000000000000' then TO_ADDRESS
      else 'direct_transfer' 
      end as called_contract
    , case when INPUT != '0x0000000000000000000000000000000000000000' then LEFT(INPUT,10)
      else 'eth_transfer' end as functions_called
    , output_actualGasCost as actualgascost
    , output_actualGasCost * p.USD_PRICE as actualgascost_usd
FROM op 
INNER JOIN {{ source('common_prices', 'token_prices_hourly_easy') }} p 
    ON p.HOUR = date_trunc('hour', call_block_time)
    AND SYMBOL = 'ETH'
    {% if is_incremental() %}
    AND p.HOUR >= CURRENT_TIMESTAMP() - interval '1 day' 
    {% endif %} 
LEFT JOIN {{ ref('erc4337_labels_bundlers') }} b ON b.address = op.bundler
LEFT JOIN {{ ref('erc4337_labels_paymasters') }} pay ON pay.address = op.bundler