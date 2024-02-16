{{ config
(
    materialized = 'incremental',
    unique_key = ['TRACE_ID','TRANSACTION_HASH']
)
}}

SELECT
    BLOCK_TIMESTAMP,
    TRANSACTION_HASH,
    FROM_ADDRESS AS bundler,
    TRACE_ID
FROM {{ source('ethereum_raw', 'traces') }} l
WHERE
    TO_ADDRESS = '0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789'
    AND ERROR IS NOT NULL
    AND OUTPUT LIKE '%41413235%' -- AA25 Nonce Error
    AND SELECTOR = '0x1fad948c'
    {% if not is_incremental() %}
        AND BLOCK_TIMESTAMP >= to_timestamp('2023-01-27', 'yyyy-MM-dd') -- first mainnet entrypoint live
    {% endif %}
    {% if is_incremental() %}
        AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}