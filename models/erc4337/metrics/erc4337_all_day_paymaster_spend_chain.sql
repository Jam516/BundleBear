{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'chain']
)
}}

SELECT
date_trunc('day', BLOCK_TIME) as DATE,
CHAIN, 
SUM(ACTUALGASCOST_USD) AS GAS_SPENT
FROM {{ ref('erc4337_all_userops') }}
WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
AND ACTUALGASCOST_USD != 'NaN'
AND ACTUALGASCOST_USD < 1000
{% if is_incremental() %}
AND BLOCK_TIME >= DATE_TRUNC('day', CURRENT_DATE()) - INTERVAL '3 day'
AND date_trunc('day', BLOCK_TIME) < date_trunc('day', CURRENT_DATE)
{% else %}
AND date_trunc('day', BLOCK_TIME) < date_trunc('day', CURRENT_DATE)
{% endif %}
GROUP BY 1,2
ORDER BY 1