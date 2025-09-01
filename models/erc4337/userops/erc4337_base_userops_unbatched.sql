{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash','tx_hash','call_index']
)
}}

with op as (
    with cbaccounts as (
        SELECT account_address
        FROM {{ ref('erc4337_base_account_deployments') }}
        WHERE factory_name = 'coinbase_smart_wallet'
    )

    , base as (
        SELECT 
            op.sender
            , op.paymaster
            , op.op_hash
            , common.udfs.js_hextoint_secure(op.output)::BIGINT/1e18 as output_actualGasCost
            , op.tx_hash
            , op.block_time
            , op.call_trace_address
            , tx.FROM_ADDRESS as bundler
            , op.executeCall
        FROM {{ ref('erc4337_base_entrypoint_call_innerhandleop') }} op
        INNER JOIN cbaccounts cb
            ON cb.account_address = op.sender
        INNER JOIN {{ source('base_raw', 'transactions') }} tx 
            ON op.block_time = tx.BLOCK_TIMESTAMP AND op.tx_hash = tx.HASH
            {% if is_incremental() %}
            AND tx.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
            AND op.block_time >= CURRENT_TIMESTAMP() - interval '3 day' 
            {% endif %}
            {% if not is_incremental() %}
            AND tx.BLOCK_TIMESTAMP >= to_timestamp('2024-04-21', 'yyyy-MM-dd') -- first mainnet entrypoint live
            AND op.block_time >= to_timestamp('2024-04-21', 'yyyy-MM-dd')
            {% endif %}
            AND op.output != ''
            AND common.udfs.js_hextoint_secure(op.output) != 'Infinity'
            AND TRY_CAST(common.udfs.js_hextoint_secure(op.output) AS FLOAT) < 1e30
    )

        SELECT
             b.*
             , t.TO_ADDRESS
             , t.INPUT
             , t.TRACE_ADDRESS
             , row_number() over (partition by b.sender, call_trace_address, tx_hash order by t.TRACE_ADDRESS asc) as call_index
        FROM base b
        INNER JOIN {{ source('base_raw', 'traces') }} t 
            ON b.block_time = t.BLOCK_TIMESTAMP
            AND b.tx_hash = t.TRANSACTION_HASH
            AND b.sender = t.FROM_ADDRESS
            AND split(b.call_trace_address, ',')[0] = split(t.TRACE_ADDRESS, ',')[0] 
            {% if not is_incremental() %}
            AND t.BLOCK_TIMESTAMP >= to_timestamp('2024-04-21', 'yyyy-MM-dd') 
            {% endif %}
            {% if is_incremental() %}
            AND t.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
            {% endif %}            
            AND ARRAY_SIZE(split(t.TRACE_ADDRESS, ',')) > 3
            AND t.CALL_TYPE != 'delegatecall'
)

SELECT 
    block_time
    , tx_hash
    , op_hash
    , sender
    , bundler
    , paymaster
    , case 
        when INPUT != '0x' then TO_ADDRESS
        when (INPUT = '0x' AND 
              -- Safe conversion with bounds check
              CASE 
                WHEN TRY_TO_DOUBLE(common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64))) > 1e30 THEN 0
                WHEN TRY_TO_DOUBLE(common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64))) IS NULL THEN 0
                ELSE TRY_TO_DECIMAL(common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64)), 38, 0)::BIGINT/1e18
              END > 0) then 'direct_transfer'
        else 'empty_call' 
        end as called_contract
    , case 
        when INPUT != '0x' then COALESCE(TEXT_SIGNATURE_SHORT, LEFT(INPUT,10))
        when (INPUT = '0x' AND 
              -- Safe conversion with bounds check
              CASE 
                WHEN TRY_TO_DOUBLE(common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64))) > 1e30 THEN 0
                WHEN TRY_TO_DOUBLE(common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64))) IS NULL THEN 0
                ELSE TRY_TO_DECIMAL(common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64)), 38, 0)::BIGINT/1e18
              END > 0) then 'eth_transfer'
        else 'empty_call' end as function_called
    , output_actualGasCost as actualgascost
    , case when INPUT != '0x' then 0
      else 
        -- Safe conversion with bounds check for the value field
        CASE 
          WHEN TRY_TO_DOUBLE(common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64))) > 1e30 THEN NULL  -- Return NULL for unrealistic values
          WHEN TRY_TO_DOUBLE(common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64))) IS NULL THEN NULL
          ELSE TRY_TO_DECIMAL(common.udfs.js_hextoint_secure(SUBSTRING(executeCall, 75, 64)), 38, 0)::BIGINT/1e18
        END
      end as value
    , call_index
FROM op 
LEFT JOIN {{ source('common_decoded', 'function_signatures') }} s ON s.HEX_SIGNATURE = LEFT(INPUT,10)