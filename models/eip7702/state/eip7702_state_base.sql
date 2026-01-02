{{ config
(
    materialized = 'table',
    copy_grants=true,
    cluster_by = ['start_day', 'chain'],
    unique_key = ['start_day', 'chain', 'authority']
)
}}

-- Step 1: Find all state changes for each authority by chain
WITH authority_state_changes AS (
  SELECT
    AUTHORITY,
    AUTHORIZED_CONTRACT,
    CHAIN,
    DATE_TRUNC('DAY', BLOCK_TIME) AS change_date
  FROM 
    {{ ref('eip7702_all_authorizations') }}
  WHERE is_valid = True
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY AUTHORITY, CHAIN, DATE_TRUNC('DAY', BLOCK_TIME) 
    ORDER BY NONCE DESC, BLOCK_TIME DESC
  ) = 1  -- Keep only the latest state for each day per chain
),

-- Step 2: Create a timeline of state changes for each authority by chain       
authority_timeline AS (
  SELECT
    AUTHORITY,
    AUTHORIZED_CONTRACT,
    CHAIN,
    change_date,
    LEAD(change_date) OVER (
      PARTITION BY AUTHORITY, CHAIN
      ORDER BY change_date
    ) AS next_change_date
  FROM
    authority_state_changes
)

-- Step 3: Build intervals (start_day inclusive, end_day inclusive)
SELECT
  AUTHORITY,
  CHAIN,
  AUTHORIZED_CONTRACT,
  change_date AS start_day,
  CASE
    WHEN next_change_date IS NULL THEN NULL
    ELSE DATEADD(DAY, -1, next_change_date)
  END AS end_day,
  CASE WHEN AUTHORIZED_CONTRACT != '0x0000000000000000000000000000000000000000'
       THEN TRUE ELSE FALSE END AS is_smart_wallet
FROM
  authority_timeline
