{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash','tx_hash']
)
}}

SELECT
    l.BLOCK_TIMESTAMP as block_time,
    l.TRANSACTION_HASH as tx_hash,
    TOPIC1 as op_hash,
    '0x' || SUBSTRING(TOPIC2, 27, 40) as account_address,
    '0x' || SUBSTRING(DATA, 27, 40) as factory,
    COALESCE(f.name, 'Unknown') as factory_name,
    t.FROM_ADDRESS as bundler,
    COALESCE(b.name, 'Unknown') as bundler_name,
    '0x' || SUBSTRING(DATA, 90, 130) as paymaster,
    COALESCE(pay.name, 'Unknown') as paymaster_name,
    (TO_DOUBLE(t.RECEIPT_GAS_USED) * TO_DOUBLE(t.GAS_PRICE))/1e18 txn_cost,
    p.USD_PRICE * (TO_DOUBLE(t.RECEIPT_GAS_USED) * TO_DOUBLE(t.GAS_PRICE))/1e18  as txn_cost_usd
FROM {{ source('linea_raw', 'logs') }} l
INNER JOIN {{ source('linea_raw', 'transactions') }} t 
    ON t.HASH = l.TRANSACTION_HASH
    AND l.TOPIC0 = '0xd51a9c61267aa6196961883ecf5ff2da6619c37dac0fa92122513fb32c032d2d' 
    AND t.TO_ADDRESS IN
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0000000071727de22e5e9d8baf0edac6f37da032')
    {% if is_incremental() %}
    AND l.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    AND t.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}
LEFT JOIN {{ ref('erc4337_labels_factories') }} f ON f.address = '0x' || SUBSTRING(l.DATA, 27, 40)
LEFT JOIN {{ ref('erc4337_labels_paymasters') }} pay ON pay.address = '0x' || SUBSTRING(l.DATA, 90, 130)
LEFT JOIN {{ ref('erc4337_labels_bundlers') }} b ON b.address = t.FROM_ADDRESS
INNER JOIN {{ source('common_prices', 'hourly') }} p 
    ON p.HOUR = date_trunc('hour', t.BLOCK_TIMESTAMP) 
    AND ADDRESS = '0x0000000000000000000000000000000000000000' 
    AND CHAIN = 'ethereum'
{% if is_incremental() %}
    AND l.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}