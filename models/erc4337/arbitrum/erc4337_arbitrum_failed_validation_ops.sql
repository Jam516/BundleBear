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
FROM {{ source('arbitrum_raw', 'traces') }} l
WHERE
    TO_ADDRESS IN 
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0576a174d229e3cfa37253523e645a78a0c91b57', 
    '0x0f46c65c17aa6b4102046935f33301f0510b163a')
    AND ERROR IS NOT NULL
    AND OUTPUT LIKE '%41413235%' -- AA25 Nonce Error
    AND SELECTOR = '0x1fad948c'
    {% if not is_incremental() %}
        AND BLOCK_TIMESTAMP >= to_timestamp('2023-01-27', 'yyyy-MM-dd') -- first mainnet entrypoint live
    {% endif %}
    {% if is_incremental() %}
        AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}