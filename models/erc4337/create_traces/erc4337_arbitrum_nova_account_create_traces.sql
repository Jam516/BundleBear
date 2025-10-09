{{ config
(
    materialized = 'incremental',
    unique_key = ['account_address']
)
}}

WITH activated_accounts AS (
    SELECT DISTINCT SENDER FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_NOVA_USEROPS
)

SELECT
    c.BLOCK_TIMESTAMP as block_time,
    c.TRANSACTION_HASH as tx_hash,
    c.ADDRESS as account_address,
    c.DEPLOYER as factory,
    COALESCE(f.name, 'unknown') as factory_name
FROM ARBITRUM_NOVA.RAW.CONTRACTS c
INNER JOIN activated_accounts a
    ON a.SENDER = c.ADDRESS
LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_FACTORIES f ON f.address = c.DEPLOYER
{% if is_incremental() %}
    AND c.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.ADDRESS ORDER BY c.BLOCK_TIMESTAMP DESC) = 1