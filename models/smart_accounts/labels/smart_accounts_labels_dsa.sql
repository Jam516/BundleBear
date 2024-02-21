{{ config
(
    materialized = 'incremental',
    unique_key = ['tx_hash', 'log_index']
)
}}

SELECT 
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as ADDRESS,
'ethereum' as blockchain
FROM ETHEREUM.RAW.LOGS
WHERE TOPIC0 = '0x83435eca805f6256e4aa778ee8b2e8aec7485fa4b643a0fff05b7df6bf688389' -- LogAccountCreated
AND ADDRESS = '0x2971adfa57b20e5a416ae5a708a8655a9c74f723'
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2020-03-25'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}

UNION ALL SELECT 
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as ADDRESS,
'polygon' as blockchain
FROM POLYGON.RAW.LOGS
WHERE TOPIC0 = '0x83435eca805f6256e4aa778ee8b2e8aec7485fa4b643a0fff05b7df6bf688389' -- LogAccountCreated
AND ADDRESS = '0xa9b99766e6c676cf1975c0d3166f96c0848ff5ad'
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2021-04-02'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}

UNION ALL SELECT 
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as ADDRESS,
'optimism' as blockchain
FROM OPTIMISM.RAW.LOGS
WHERE TOPIC0 = '0x83435eca805f6256e4aa778ee8b2e8aec7485fa4b643a0fff05b7df6bf688389' -- LogAccountCreated
AND ADDRESS = '0x6ce3e607c808b4f4c26b7f6adaeb619e49cabb25'
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2022-01-12'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}

UNION ALL SELECT 
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as ADDRESS,
'arbitrum' as blockchain
FROM ARBITRUM.RAW.LOGS
WHERE TOPIC0 = '0x83435eca805f6256e4aa778ee8b2e8aec7485fa4b643a0fff05b7df6bf688389' -- LogAccountCreated
AND ADDRESS = '0x1ee00c305c51ff3be60162456a9b533c07cd9288'
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2021-09-13'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}