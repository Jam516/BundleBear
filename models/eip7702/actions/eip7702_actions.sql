{{ config
(
    materialized = 'table',
    copy_grants=true,
    cluster_by = ['block_date', 'chain'],
    unique_key = ['block_time', 'chain', 'tx_hash', 'op_hash']
)
}}

WITH uops AS (
SELECT 
'erc-4337 userop' AS TYPE,
BLOCK_TIME,
date_trunc('day', BLOCK_TIME) AS block_date,
TX_HASH,
SENDER AS FROM_ADDRESS,
CALLED_CONTRACT AS TO_ADDRESS,
AUTHORIZED_CONTRACT,
VALUE,
u.CHAIN,
OP_HASH
FROM {{ ref('erc4337_all_userops') }} u
INNER JOIN {{ ref('eip7702_state_base') }} s
ON u.SENDER = s.AUTHORITY
AND DATE_TRUNC('day',BLOCK_TIME) = s.DAY
AND IS_SMART_WALLET = TRUE
AND u.CHAIN = s.CHAIN
{% if is_incremental() %}
AND BLOCK_TIME >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
)

, self_initiated AS (
SELECT
CASE WHEN FROM_ADDRESS = TO_ADDRESS THEN 'self-initated txn' 
ELSE 'eoa txn'
END as TYPE,
BLOCK_TIMESTAMP AS BLOCK_TIME,
date_trunc('day', BLOCK_TIMESTAMP) AS block_date,
HASH AS TX_HASH,
FROM_ADDRESS,
TO_ADDRESS,
AUTHORIZED_CONTRACT,
VALUE,
s.CHAIN,
null AS OP_HASH
FROM {{ source('base_raw', 'transactions') }} t
INNER JOIN {{ ref('eip7702_state_base') }} s
ON t.FROM_ADDRESS = s.AUTHORITY
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) = s.DAY
AND IS_SMART_WALLET = TRUE
AND s.CHAIN = 'base'
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}

UNION ALL
SELECT
CASE WHEN FROM_ADDRESS = TO_ADDRESS THEN 'self-initated txn' 
ELSE 'eoa txn'
END as TYPE,
BLOCK_TIMESTAMP AS BLOCK_TIME,
date_trunc('day', BLOCK_TIMESTAMP) AS block_date,
HASH AS TX_HASH,
FROM_ADDRESS,
TO_ADDRESS,
AUTHORIZED_CONTRACT,
VALUE,
s.CHAIN,
null AS OP_HASH
FROM {{ source('bsc_raw', 'transactions') }} t
INNER JOIN {{ ref('eip7702_state_base') }} s
ON t.FROM_ADDRESS = s.AUTHORITY
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) = s.DAY
AND IS_SMART_WALLET = TRUE
AND s.CHAIN = 'bsc'
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}

UNION ALL
SELECT
CASE WHEN FROM_ADDRESS = TO_ADDRESS THEN 'self-initated txn' 
ELSE 'eoa txn'
END as TYPE,
BLOCK_TIMESTAMP AS BLOCK_TIME,
date_trunc('day', BLOCK_TIMESTAMP) AS block_date,
HASH AS TX_HASH,
FROM_ADDRESS,
TO_ADDRESS,
AUTHORIZED_CONTRACT,
VALUE,
s.CHAIN,
null AS OP_HASH
FROM {{ source('optimism_raw', 'transactions') }} t
INNER JOIN {{ ref('eip7702_state_base') }} s
ON t.FROM_ADDRESS = s.AUTHORITY
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) = s.DAY
AND IS_SMART_WALLET = TRUE
AND s.CHAIN = 'optimism'
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}

UNION ALL
SELECT
CASE WHEN FROM_ADDRESS = TO_ADDRESS THEN 'self-initated txn' 
ELSE 'eoa txn'
END as TYPE,
BLOCK_TIMESTAMP AS BLOCK_TIME,
date_trunc('day', BLOCK_TIMESTAMP) AS block_date,
HASH AS TX_HASH,
FROM_ADDRESS,
TO_ADDRESS,
AUTHORIZED_CONTRACT,
VALUE,
s.CHAIN,
null AS OP_HASH
FROM {{ source('ethereum_raw', 'transactions') }} t
INNER JOIN {{ ref('eip7702_state_base') }} s
ON t.FROM_ADDRESS = s.AUTHORITY
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) = s.DAY
AND IS_SMART_WALLET = TRUE
AND s.CHAIN = 'ethereum'
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}

UNION ALL
SELECT
CASE WHEN FROM_ADDRESS = TO_ADDRESS THEN 'self-initated txn' 
ELSE 'eoa txn'
END as TYPE,
BLOCK_TIMESTAMP AS BLOCK_TIME,
date_trunc('day', BLOCK_TIMESTAMP) AS block_date,
HASH AS TX_HASH,
FROM_ADDRESS,
TO_ADDRESS,
AUTHORIZED_CONTRACT,
VALUE,
s.CHAIN,
null AS OP_HASH
FROM {{ source('gnosis_raw', 'transactions') }} t
INNER JOIN {{ ref('eip7702_state_base') }} s
ON t.FROM_ADDRESS = s.AUTHORITY
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) = s.DAY
AND IS_SMART_WALLET = TRUE
AND s.CHAIN = 'gnosis'
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
)

SELECT * FROM uops
UNION ALL
SELECT * FROM self_initiated