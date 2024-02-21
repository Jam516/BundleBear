{{ config
(
    materialized = 'incremental',
    unique_key = 'UNIQUE_ID'
)
}}

SELECT 
b.ADDRESS,
TOKEN_ADDRESS,
TOKEN_SYMBOL,
BALANCE,
USD_BALANCE,
BLOCK_TIMESTAMP,
date_trunc('month',BLOCK_TIMESTAMP) AS MONTH,
UNIQUE_ID
FROM ETHEREUM.ASSETS.ETH_AND_ERC20_BALANCES b
INNER JOIN {{ ref('smart_accounts_labels_dsa') }} a 
ON a.BLOCKCHAIN = 'ethereum'
AND a.ADDRESS = b.ADDRESS
{% if not is_incremental() %}
AND b.BLOCK_TIMESTAMP >= '2020-03-25'::DATE
{% endif %}
{% if is_incremental() %}
AND b.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
ORDER BY BLOCK_TIMESTAMP ASC
