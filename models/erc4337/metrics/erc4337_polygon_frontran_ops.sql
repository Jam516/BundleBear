{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash','failed_txn']
)
}}

with failed_handleop AS (
SELECT
    BLOCK_TIMESTAMP,
    TRANSACTION_HASH,
    FROM_ADDRESS AS bundler,
    COALESCE(b.name, 'Unknown') as bundler_name,
    TRACE_ID
FROM {{ source('polygon_raw', 'traces') }} l
LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_BUNDLERS b ON b.address = l.FROM_ADDRESS
WHERE
    TO_ADDRESS IN 
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0000000071727de22e5e9d8baf0edac6f37da032')
    AND ERROR IS NOT NULL
    AND OUTPUT LIKE ANY ('%41413235%', '%41413130%') -- AA25 AA10
    AND SELECTOR = '0x1fad948c'
    {% if not is_incremental() %}
    AND BLOCK_TIMESTAMP >= to_timestamp('2023-05-01', 'yyyy-MM-dd') 
    {% endif %}
    {% if is_incremental() %}
    AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}
)

, failed_op AS (
    SELECT 
    t.TRANSACTION_HASH,
    t.INPUT_PARAMS:"userOpHash"::STRING as op_hash,
    f.bundler,
    f.bundler_name
    FROM {{ source('polygon_decoded', 'traces') }} t
    INNER JOIN failed_handleop f 
    ON f.TRANSACTION_HASH = t.TRANSACTION_HASH
    AND FROM_ADDRESS IN 
    ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789', 
    '0x0000000071727de22e5e9d8baf0edac6f37da032')
    AND NAME = 'validateUserOp'
    {% if not is_incremental() %}
    AND t.BLOCK_TIMESTAMP >= to_timestamp('2023-05-01', 'yyyy-MM-dd') 
    {% endif %}
    {% if is_incremental() %}
    AND t.BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}
)

, op_overlap AS (
    SELECT
    u.BLOCK_TIME,
    u.TX_HASH AS successful_txn,
    f.TRANSACTION_HASH AS failed_txn,
    u.OP_HASH,
    u.BUNDLER AS successful_bundler,
    u.BUNDLER_NAME AS successful_bundler_name,
    f.BUNDLER AS failed_bundler,
    f.BUNDLER_NAME AS failed_bundler_name,
    FROM {{ ref('erc4337_polygon_userops') }} u
    INNER JOIN failed_op f ON f.op_hash = u.op_hash
    {% if not is_incremental() %}
    AND u.BLOCK_TIME >= to_timestamp('2023-05-01', 'yyyy-MM-dd') 
    {% endif %}
    {% if is_incremental() %}
    AND u.BLOCK_TIME >= CURRENT_TIMESTAMP() - interval '3 day' 
    {% endif %}
)

SELECT * 
FROM op_overlap 
WHERE successful_bundler != failed_bundler 
AND successful_bundler_name = 'Unknown'