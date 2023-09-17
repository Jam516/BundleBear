{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash','tx_hash']
)
}}

SELECT
    l.BLOCK_TIMESTAMP as block_time,
    l.TRANSACTION_HASH as tx_hash,
    l.PARAMS:"userOpHash"::STRING as op_hash,
    l.PARAMS:"sender"::STRING as account_address,
    l.PARAMS:"factory"::STRING as factory,
    COALESCE(f.name, 'Unknown') as factory_name,
    t.FROM_ADDRESS as bundler,
    COALESCE(b.name, 'Unknown') as bundler_name,
    l.PARAMS:"paymaster"::STRING as paymaster,
    COALESCE(pay.name, 'Unknown') as paymaster_name
FROM {{ source('optimism_decoded', 'logs_sample') }} l
INNER JOIN {{ source('optimism_raw', 'transactions') }} t 
    ON t.HASH = l.TRANSACTION_HASH
    AND l.NAME = 'AccountDeployed' 
    AND t.TO_ADDRESS IN
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0576a174d229e3cfa37253523e645a78a0c91b57', 
    '0x0f46c65c17aa6b4102046935f33301f0510b163a')
LEFT JOIN {{ ref('erc4337_labels_factories') }} f ON f.address = l.PARAMS:"factory"
LEFT JOIN {{ ref('erc4337_labels_paymasters') }} pay ON pay.address = l.PARAMS:"paymaster"
LEFT JOIN {{ ref('erc4337_labels_bundlers') }} b ON b.address = t.FROM_ADDRESS
{% if is_incremental() %}
WHERE l.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}