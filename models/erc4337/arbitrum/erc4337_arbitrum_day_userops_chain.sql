{{ config
(
    materialized = 'table'
)
}}

SELECT
date_trunc('day', BLOCK_TIME) as DATE,
COUNT(*) as NUM_USEROPS
FROM {{ ref('erc4337_arbitrum_userops') }}
WHERE date_trunc('day', BLOCK_TIME) < date_trunc('day', CURRENT_DATE)
-- {% if is_incremental() %}
-- AND BLOCK_TIME >= CURRENT_TIMESTAMP() - interval '3 day' 
-- {% endif %}
GROUP BY 1
ORDER BY 1