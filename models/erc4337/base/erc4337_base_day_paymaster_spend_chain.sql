{{ config
(
    materialized = 'incremental',
    unique_key = ['DATE']
)
}}

SELECT
date_trunc('day', BLOCK_TIME) as DATE,
SUM(ACTUALGASCOST_USD) AS GAS_SPENT
FROM {{ ref('erc4337_base_userops') }}
WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
AND ACTUALGASCOST_USD != 'NaN'
AND ACTUALGASCOST_USD < 1000
AND date_trunc('day', BLOCK_TIME) < date_trunc('day', CURRENT_DATE)
{% if is_incremental() %}
AND date_trunc('day', BLOCK_TIME) >= CURRENT_DATE - interval '3 day' 
{% endif %}
GROUP BY 1
ORDER BY 1