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
FROM {{ source('bsc_raw', 'traces') }} l
LEFT JOIN {{ ref('erc4337_labels_bundlers') }} b ON b.address = l.FROM_ADDRESS
WHERE
    TO_ADDRESS IN 
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0000000071727de22e5e9d8baf0edac6f37da032',
    '0x4337084d9e255ff0702461cf8895ce9e3b5ff108')
    AND ERROR IS NOT NULL
    AND OUTPUT LIKE ANY ('%41413235%', '%41413130%') -- AA25 Nonce Error
    AND SELECTOR = '0x1fad948c'
    {% if not is_incremental() %}
        AND BLOCK_TIMESTAMP >= to_timestamp('2023-01-27', 'yyyy-MM-dd') -- first mainnet entrypoint live
    {% endif %}
    {% if is_incremental() %}
        AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}