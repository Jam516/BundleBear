{{ config
(
    materialized = 'incremental',
    unique_key = ['chain', 'sender'],
    copy_grants=true
)
}}

SELECT
    DATE_TRUNC('day', u.BLOCK_TIME) AS DATE,
    u.CHAIN,
    CASE 
        WHEN u.CALLED_CONTRACT IN (
            '0xa581c4a4db7175302464ff3c06380bc3270b4037',
            '0x75cf11467937ce3f2f357ce24ffc3dbf8fd5c226'
            ) THEN 'Safe4337Module'
        WHEN d.ACCOUNT_ADDRESS IS NOT NULL AND d.FACTORY_NAME != 'unknown'
            THEN 'factory - ' || d.FACTORY_NAME
        WHEN d.ACCOUNT_ADDRESS IS NOT NULL AND d.FACTORY_NAME = 'unknown'
            THEN 'factory - ' || d.FACTORY
        WHEN a.OP_HASH IS NOT NULL THEN 'eip7702 - ' || COALESCE(al.NAME, a.AUTHORIZED_CONTRACT)
        ELSE 'unknown'
    END AS PROVIDER,
    u.SENDER
FROM {{ ref('erc4337_all_first_userops') }} u
LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_CREATE_TRACES d
    ON u.SENDER = d.ACCOUNT_ADDRESS
    AND u.CHAIN = d.CHAIN
LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS a
    ON a.OP_HASH = u.OP_HASH 
    AND a.CHAIN = u.CHAIN
LEFT JOIN BUNDLEBEAR.DBT_KOFI.EIP7702_LABELS_AUTHORIZED_CONTRACTS al
    ON al.ADDRESS = a.AUTHORIZED_CONTRACT
{% if is_incremental() %}
WHERE u.BLOCK_TIME >= CURRENT_TIMESTAMP() - INTERVAL '3 day'
{% endif %}