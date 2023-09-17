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
        CASE WHEN GAS_PRICE = 0 THEN 0 
        ELSE (RECEIPT_L1_FEE + (RECEIPT_GAS_USED*GAS_PRICE)) / 1e18
        END AS bundler_outflow,
        p.USD_PRICE * 
        (CASE WHEN GAS_PRICE = 0 THEN 0 
        ELSE (RECEIPT_L1_FEE + (RECEIPT_GAS_USED*GAS_PRICE)) / 1e18 
        END) as bundler_outflow_usd
    FROM {{ source('optimism_raw', 'transactions') }} t
    INNER JOIN {{ source('common_prices', 'token_prices_hourly_easy') }} p 
        ON p.HOUR = date_trunc('hour', t.BLOCK_TIMESTAMP) 
        AND t.TO_ADDRESS IN
        ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
        '0x0576a174d229e3cfa37253523e645a78a0c91b57', 
        '0x0f46c65c17aa6b4102046935f33301f0510b163a')
        AND LEFT(INPUT,10) = '0x1fad948c'   
        AND SYMBOL = 'ETH'        
),

input AS (
    SELECT 
    TX_HASH,
    SUM(ACTUALGASCOST) as bundler_inflow,
    SUM(ACTUALGASCOST_USD) as bundler_inflow_usd
    FROM {{ ref('erc4337_optimism_userops') }}
    GROUP BY 1
) 

SELECT 
op.block_time, 
op.tx_hash,
op.bundler,
COALESCE(b.name, 'Unknown') as bundler_name,
i.bundler_inflow,
i.bundler_inflow_usd,
op.bundler_outflow,
op.bundler_outflow_usd,
bundler_inflow - bundler_outflow as bundler_revenue,
bundler_inflow_usd - bundler_outflow_usd as bundler_revenue_usd,
op.token
FROM output op
INNER JOIN input i
    ON i.TX_HASH = op.TX_HASH
    {% if is_incremental() %}
    AND op.block_time >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}
LEFT JOIN {{ ref('erc4337_labels_bundlers') }} b ON b.address = op.bundler