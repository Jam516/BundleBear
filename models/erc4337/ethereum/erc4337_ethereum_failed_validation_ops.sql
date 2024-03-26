{{ config
(
    materialized = 'incremental',
    unique_key = ['TRACE_ID','TX_HASH']
)
}}

SELECT
    BLOCK_TIMESTAMP AS block_time,
    TRANSACTION_HASH AS tx_hash,
    FROM_ADDRESS AS bundler,
    COALESCE(b.name, 'Unknown') as bundler_name,
    TRACE_ID
FROM {{ source('ethereum_raw', 'traces') }} l
LEFT JOIN {{ ref('erc4337_labels_bundlers') }} b ON b.address = l.FROM_ADDRESS
WHERE
    TO_ADDRESS IN 
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0576a174d229e3cfa37253523e645a78a0c91b57', 
    '0x0f46c65c17aa6b4102046935f33301f0510b163a',
    '0x0000000071727de22e5e9d8baf0edac6f37da032')
    AND ERROR IS NOT NULL
    AND OUTPUT LIKE '%41413235%' -- AA25 Nonce Error
    AND SELECTOR = '0x1fad948c'
    {% if not is_incremental() %}
        AND BLOCK_TIMESTAMP >= to_timestamp('2023-01-27', 'yyyy-MM-dd') -- first mainnet entrypoint live
    {% endif %}
    {% if is_incremental() %}
        AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}