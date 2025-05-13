{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

-- MIGHT RETIRE THIS ONE. NOT RELEVANT.

-- Step 1: Find all state changes for each authority (days when a transaction occurred)
WITH authority_state_changes AS (
  SELECT
    AUTHORITY,
    AUTHORIZED_CONTRACT,
    DATE_TRUNC('DAY', BLOCK_TIME) AS change_date
  FROM 
    {{ ref('eip7702_all_authorizations') }}
  WHERE CHAIN_ID = 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY AUTHORITY, DATE_TRUNC('DAY', BLOCK_TIME) 
    ORDER BY NONCE DESC, BLOCK_TIME DESC
  ) = 1  -- Keep only the latest state for each day
),

-- Step 2: For each authority, create a timeline of their state changes
authority_timeline AS (
  SELECT
    AUTHORITY,
    AUTHORIZED_CONTRACT,
    change_date,
    LEAD(change_date) OVER (
      PARTITION BY AUTHORITY 
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
)

-- Step 4: Join dates with authority states and calculate metrics
SELECT
  d.day,
  'cross-chain' AS chain,
  COUNT(DISTINCT CASE 
    WHEN at.AUTHORIZED_CONTRACT != '0x0000000000000000000000000000000000000000' 
    THEN at.AUTHORITY 
  END) AS live_smart_wallets,
  COUNT(DISTINCT CASE 
    WHEN at.AUTHORIZED_CONTRACT != '0x0000000000000000000000000000000000000000' 
    THEN at.AUTHORIZED_CONTRACT 
  END) AS live_authorized_contracts
FROM 
  date_spine d
LEFT JOIN
  authority_timeline at
ON
  d.day >= at.change_date
  AND (d.day < at.next_change_date OR at.next_change_date IS NULL)
GROUP BY
  1, 2
ORDER BY
  1

--   Note: I could add cross-chain authorizations to the single chain table by making each cross-chain auth into multiple rows, one for each chain.
--   so every cross-chain auth is counted as one addition to all chains. considering it.