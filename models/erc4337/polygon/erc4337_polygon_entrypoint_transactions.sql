{{ config
(
    materialized = 'incremental',
    unique_key = 'tx_hash'
)
}}

with handleop AS (
    SELECT
        t.BLOCK_TIMESTAMP as block_time,
        t.HASH as tx_hash,
        row_number() over (partition by tr.TRANSACTION_HASH order by tr.TRACE_ADDRESS desc) as ranks,
        t.FROM_ADDRESS AS bundler,
        'ETH' AS token,
        TO_DOUBLE(tr.VALUE)/1e18 as bundler_inflow,
        (TO_DOUBLE(t.RECEIPT_GAS_USED) * TO_DOUBLE(t.GAS_PRICE))/1e18 as bundler_outflow
    FROM {{ source('polygon_raw', 'transactions') }} t
    INNER JOIN {{ source('polygon_raw', 'traces') }} tr 
        ON t.HASH = tr.TRANSACTION_HASH
        AND t.TO_ADDRESS IN
        ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
        '0x0576a174d229e3cfa37253523e645a78a0c91b57', 
        '0x0f46c65c17aa6b4102046935f33301f0510b163a')
        AND tr.FROM_ADDRESS IN
        ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
        '0x0576a174d229e3cfa37253523e645a78a0c91b57', 
        '0x0f46c65c17aa6b4102046935f33301f0510b163a')
        AND tr.VALUE != '0'
        AND tr.VALUE IS NOT NULL
        AND LEFT(t.INPUT,10) = '0x1fad948c'        
) 

SELECT 
op.block_time, 
op.tx_hash,
op.bundler,
COALESCE(b.name, 'Unknown') as bundler_name,
op.bundler_inflow,
bundler_inflow * p.USD_PRICE as bundler_inflow_usd,
op.bundler_outflow,
bundler_outflow * p.USD_PRICE as bundler_outflow_usd,
bundler_inflow - bundler_outflow as bundler_revenue,
(bundler_inflow - bundler_outflow) * p.USD_PRICE as bundler_revenue_usd,
op.token
FROM handleop op
INNER JOIN {{ source('common_prices', 'token_prices_hourly_easy') }} p 
    ON op.ranks = 1
    AND p.HOUR = date_trunc('hour', op.block_time)
    AND SYMBOL = op.token
    {% if is_incremental() %}
    AND op.block_time >= CURRENT_TIMESTAMP() - interval '1 day' 
    {% endif %}
LEFT JOIN {{ ref('erc4337_labels_bundlers') }} b ON b.address = op.bundler