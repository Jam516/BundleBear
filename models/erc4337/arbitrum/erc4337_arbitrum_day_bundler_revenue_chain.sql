{{ config
(
    materialized = 'incremental',
    unique_key = ['DATE']
)
}}

SELECT
date_trunc('day', BLOCK_TIME) as DATE,
SUM(BUNDLER_REVENUE_USD) AS REVENUE
FROM {{ ref('erc4337_arbitrum_entrypoint_transactions') }}
WHERE BUNDLER_REVENUE_USD != 'NaN'
AND BUNDLER_REVENUE_USD < 1000000000
AND date_trunc('day', BLOCK_TIME) < date_trunc('day', CURRENT_DATE)
{% if is_incremental() %}
AND date_trunc('day', BLOCK_TIME) >= CURRENT_DATE - interval '3 day' 
{% endif %}
GROUP BY 1
ORDER BY 1