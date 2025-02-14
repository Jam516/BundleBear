{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT
date_trunc('day', BLOCK_TIME) as DATE,
CHAIN,
SUM(BUNDLER_REVENUE_USD) AS REVENUE
FROM {{ ref('erc4337_all_entrypoint_transactions') }}
WHERE BUNDLER_REVENUE_USD != 'NaN'
AND BUNDLER_REVENUE_USD < 1000000000
AND date_trunc('day', BLOCK_TIME) < date_trunc('day', CURRENT_DATE)
GROUP BY 1,2
ORDER BY 1