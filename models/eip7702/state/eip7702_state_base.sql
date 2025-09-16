{{ config
(
    materialized = 'table',
    copy_grants=true,
    cluster_by = ['day', 'chain'],
    unique_key = ['day', 'chain', 'authority']
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
),

-- Step 3: Generate date spine from earliest change to today
date_spine AS (
  SELECT DATEADD(DAY, SEQ4(), (SELECT MIN(change_date) FROM authority_state_changes)) AS day
  FROM TABLE(GENERATOR(ROWCOUNT => 1000))
  QUALIFY day <= CURRENT_DATE()
),

-- Step 4: Get unique chains for cross join
chains AS (
  SELECT DISTINCT CHAIN
  FROM {{ ref('eip7702_all_authorizations') }}
),

-- Step 5: Create day-chain combinations
day_chain_combinations AS (
  SELECT
    d.day,
    c.CHAIN
  FROM date_spine d
  CROSS JOIN chains c
)

-- Step 6: Join with authority timelines to create daily state
SELECT
  dc.day,
  dc.CHAIN,
  at.AUTHORITY,
  at.AUTHORIZED_CONTRACT,
  CASE WHEN at.AUTHORIZED_CONTRACT != '0x0000000000000000000000000000000000000000' 
       THEN TRUE ELSE FALSE END AS is_smart_wallet
FROM 
  day_chain_combinations dc
LEFT JOIN
  authority_timeline at
ON
  dc.day >= at.change_date
  AND (dc.day < at.next_change_date OR at.next_change_date IS NULL)
  AND dc.CHAIN = at.CHAIN
WHERE
  at.AUTHORITY IS NOT NULL  -- Only include days where we have authority data