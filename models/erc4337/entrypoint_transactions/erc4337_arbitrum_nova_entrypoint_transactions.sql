{{ config
(
    materialized = 'incremental',
    unique_key = 'tx_hash'
)
}}

with output AS (
    SELECT
        BLOCK_TIMESTAMP as block_time,
        HASH as tx_hash,
        FROM_ADDRESS AS bundler,
        'ETH' AS token,
        (TO_DOUBLE(RECEIPT_GAS_USED) * TO_DOUBLE(RECEIPT_EFFECTIVE_GAS_PRICE))/1e18 as bundler_outflow,
        p.PRICE * (TO_DOUBLE(RECEIPT_GAS_USED) * TO_DOUBLE(RECEIPT_EFFECTIVE_GAS_PRICE))/1e18 as bundler_outflow_usd
    FROM {{ source('arbitrum_nova_raw', 'transactions') }} t
    INNER JOIN {{ source('common_prices', 'hourly') }} p 
        ON p.TIMESTAMP = date_trunc('hour', t.BLOCK_TIMESTAMP) 
        AND p.ADDRESS = '0x0000000000000000000000000000000000000000' 
        AND p.CHAIN = 'ethereum'
        AND t.TO_ADDRESS IN
        ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0000000071727de22e5e9d8baf0edac6f37da032',
    '0x4337084d9e255ff0702461cf8895ce9e3b5ff108')
        AND LEFT(INPUT,10) = '0x1fad948c'   
        {% if is_incremental() %}
        AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
        {% endif %}              
),

input AS (
    SELECT 
    BLOCK_TIME,
    TX_HASH,
    SUM(ACTUALGASCOST) as bundler_inflow,
    SUM(ACTUALGASCOST_USD) as bundler_inflow_usd,
    COUNT(*) AS num_userops
    FROM {{ ref('erc4337_arbitrum_nova_userops') }}
    {% if is_incremental() %}
    WHERE BLOCK_TIME >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %} 
    GROUP BY 1,2
)


SELECT 
op.block_time, 
op.tx_hash,
op.bundler,
COALESCE(b.name, 'Unknown') as bundler_name,
COALESCE(i.bundler_inflow,0) as bundler_inflow,
COALESCE(i.bundler_inflow_usd,0) as bundler_inflow_usd,
op.bundler_outflow,
op.bundler_outflow_usd,
COALESCE(bundler_inflow,0) - bundler_outflow as bundler_revenue,
COALESCE(bundler_inflow_usd,0) - bundler_outflow_usd as bundler_revenue_usd,
op.token,
COALESCE(i.num_userops,0) as num_userops
FROM output op
LEFT JOIN input i
    ON i.TX_HASH = op.TX_HASH
LEFT JOIN {{ ref('erc4337_labels_bundlers') }} b ON b.address = op.bundler
