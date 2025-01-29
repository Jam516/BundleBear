{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash','tx_hash']
)
}}

SELECT
    l.BLOCK_TIMESTAMP as block_time,
    l.TRANSACTION_HASH as tx_hash,
    -- REGEXP_SUBSTR(l.PARAMS, 'userOpHash": "(.*?)", "sender', 1, 1, 'e') as op_hash,
    -- REGEXP_SUBSTR(l.PARAMS, 'sender": "(.*?)", "factory', 1, 1, 'e') as account_address,
    -- REGEXP_SUBSTR(l.PARAMS, 'factory": "(.*?)", "paymaster', 1, 1, 'e') as factory,
    l.PARAMS:"userOpHash"::STRING as op_hash,
    l.PARAMS:"sender"::STRING as account_address,
    l.PARAMS:"factory"::STRING as factory,
    COALESCE(f.name, 'Unknown') as factory_name,
    t.FROM_ADDRESS as bundler,
    COALESCE(b.name, 'Unknown') as bundler_name,
    -- REGEXP_SUBSTR(l.PARAMS, 'paymaster": "(.*?)"\\}', 1, 1, 'e') as paymaster,
    l.PARAMS:"paymaster"::STRING as paymaster,
    COALESCE(pay.name, 'Unknown') as paymaster_name,
    CASE WHEN t.GAS_PRICE = 0 THEN 0 
    ELSE (t.RECEIPT_L1_FEE + (t.RECEIPT_GAS_USED*t.GAS_PRICE)) / 1e18
    END AS txn_cost,
    p.PRICE * 
    (CASE WHEN t.GAS_PRICE = 0 THEN 0 
    ELSE (t.RECEIPT_L1_FEE + (t.RECEIPT_GAS_USED*GAS_PRICE)) / 1e18 
    END) as txn_cost_usd
FROM {{ source('base_decoded', 'logs') }} l
INNER JOIN {{ source('base_raw', 'transactions') }} t 
    ON t.HASH = l.TRANSACTION_HASH
    AND l.NAME = 'AccountDeployed' 
    AND t.TO_ADDRESS IN
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0576a174d229e3cfa37253523e645a78a0c91b57', 
    '0x0f46c65c17aa6b4102046935f33301f0510b163a',
    '0x0000000071727de22e5e9d8baf0edac6f37da032')
LEFT JOIN {{ ref('erc4337_labels_factories') }} f ON f.address = l.PARAMS:"factory"::STRING
LEFT JOIN {{ ref('erc4337_labels_paymasters') }} pay ON pay.address = l.PARAMS:"paymaster"::STRING
LEFT JOIN {{ ref('erc4337_labels_bundlers') }} b ON b.address = t.FROM_ADDRESS
INNER JOIN {{ source('common_prices', 'hourly') }} p 
    ON p.TIMESTAMP = date_trunc('hour', t.BLOCK_TIMESTAMP) 
    AND ADDRESS = '0x0000000000000000000000000000000000000000' 
    AND CHAIN = 'ethereum'
{% if is_incremental() %}
    AND l.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}