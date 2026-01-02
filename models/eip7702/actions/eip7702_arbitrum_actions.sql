{{ config
(
    materialized = 'incremental',
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
OP_HASH,
CASE WHEN FUNCTION_CALLED = 'eth_transfer' THEN 1
ELSE 0
END as is_eth_transfer
FROM {{ ref('erc4337_all_userops') }} u
INNER JOIN {{ ref('eip7702_state_base') }} s
ON u.SENDER = s.AUTHORITY
AND DATE_TRUNC('day', BLOCK_TIME) >= s.START_DAY
AND (s.END_DAY IS NULL OR DATE_TRUNC('day', BLOCK_TIME) <= s.END_DAY)
AND IS_SMART_WALLET = TRUE
AND u.CHAIN = 'arbitrum'
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
null AS OP_HASH,
CASE WHEN t.INPUT = '0x' THEN 1
ELSE 0
END as is_eth_transfer
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN {{ ref('eip7702_state_base') }} s
ON t.FROM_ADDRESS = s.AUTHORITY
AND DATE_TRUNC('day', BLOCK_TIMESTAMP) >= s.START_DAY
AND (s.END_DAY IS NULL OR DATE_TRUNC('day', BLOCK_TIMESTAMP) <= s.END_DAY)
AND IS_SMART_WALLET = TRUE
AND s.CHAIN = 'arbitrum'
{% if is_incremental() %}
AND t.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
AND t.BLOCK_TIMESTAMP >= DATE('2025-05-05')
)

, relayed_actions AS (
SELECT
'relayed action' AS TYPE,
t.BLOCK_TIMESTAMP AS BLOCK_TIME,
date_trunc('day', t.BLOCK_TIMESTAMP) AS BLOCK_DATE,
t.TRANSACTION_HASH AS TX_HASH,
t.FROM_ADDRESS,
t.TO_ADDRESS,
s.AUTHORIZED_CONTRACT,
t.VALUE,
s.CHAIN,
null AS OP_HASH,
CASE WHEN t.INPUT = '0x' THEN 1
ELSE 0
END as is_eth_transfer
FROM {{ source('arbitrum_raw', 'traces') }} t
INNER JOIN {{ ref('eip7702_state_base') }} s
ON t.FROM_ADDRESS = s.AUTHORITY
AND DATE_TRUNC('day', t.BLOCK_TIMESTAMP) >= s.START_DAY
AND (s.END_DAY IS NULL OR DATE_TRUNC('day', t.BLOCK_TIMESTAMP) <= s.END_DAY)
AND s.IS_SMART_WALLET = TRUE
AND s.CHAIN = 'arbitrum'
AND t.CALL_TYPE = 'call'
INNER JOIN {{ source('arbitrum_raw', 'transactions') }} tx
ON tx.HASH = t.TRANSACTION_HASH
AND tx.TO_ADDRESS NOT IN 
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0000000071727de22e5e9d8baf0edac6f37da032',
    '0x4337084d9e255ff0702461cf8895ce9e3b5ff108')
AND tx.FROM_ADDRESS != s.AUTHORITY
WHERE 
{% if is_incremental() %}
t.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
AND tx.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
{% if not is_incremental() %}
 t.BLOCK_TIMESTAMP >= DATE('2025-05-05')
AND tx.BLOCK_TIMESTAMP >= DATE('2025-05-05')
{% endif %}
AND t.BLOCK_TIMESTAMP < CURRENT_DATE()
AND tx.BLOCK_TIMESTAMP < CURRENT_DATE()
)

SELECT * FROM uops
UNION ALL
SELECT * FROM self_initiated
UNION ALL
SELECT * FROM relayed_actions
