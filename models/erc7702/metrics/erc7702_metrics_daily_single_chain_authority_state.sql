{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

-- Step 1: Find the latest state for each authority on each day (including CHAIN)
WITH authority_daily_states AS (
  SELECT
    AUTHORITY,
    AUTHORIZED_CONTRACT,
    CHAIN,  
    DATE_TRUNC('DAY', BLOCK_TIME) AS state_date,
    ROW_NUMBER() OVER (
      PARTITION BY AUTHORITY, DATE_TRUNC('DAY', BLOCK_TIME) 
      ORDER BY NONCE DESC, BLOCK_TIME DESC
    ) AS daily_rank
  FROM 
    {{ ref('erc7702_all_authorizations') }}
  WHERE CHAIN_ID != 0
),

-- Step 2: Keep only the latest state for each day
latest_daily_states AS (
  SELECT
    AUTHORITY,
    AUTHORIZED_CONTRACT,
    CHAIN,  
    state_date
  FROM
    authority_daily_states
  WHERE
    daily_rank = 1
),

-- Step 3: Determine validity periods for each state 
authority_state_periods AS (
  SELECT
    AUTHORITY,
    AUTHORIZED_CONTRACT,
    CHAIN, 
    state_date AS start_date,
    LEAD(state_date, 1, DATEADD(DAY, 1, CURRENT_DATE())) OVER (
      PARTITION BY AUTHORITY, CHAIN  
      ORDER BY state_date
    ) AS end_date
  FROM 
    latest_daily_states
),

-- Step 4: Generate date spine
date_spine AS (
  SELECT DATEADD(DAY, SEQ4(), (SELECT MIN(state_date) FROM latest_daily_states)) AS day
  FROM TABLE(GENERATOR(ROWCOUNT => 1000))
  QUALIFY day <= CURRENT_DATE()
),

-- Step 5: Get unique chains for cross join
chains AS (
  SELECT DISTINCT CHAIN
  FROM BUNDLEBEAR.DBT_KOFI.ERC7702_ALL_AUTHORIZATIONS
),

-- Step 6: Create day-chain combinations
day_chain_combinations AS (
  SELECT
    d.day,
    c.CHAIN
  FROM date_spine d
  CROSS JOIN chains c
)

-- Step 7: Calculate metrics by day and chain
SELECT
  dc.day,
  dc.CHAIN,
  COUNT(DISTINCT CASE WHEN asp.AUTHORIZED_CONTRACT != '0x0000000000000000000000000000000000000000' THEN asp.AUTHORITY END) AS live_smart_wallets,
  COUNT(DISTINCT CASE WHEN asp.AUTHORIZED_CONTRACT != '0x0000000000000000000000000000000000000000' THEN asp.AUTHORIZED_CONTRACT END) AS live_authorized_contracts
FROM 
  day_chain_combinations dc
LEFT JOIN
  authority_state_periods asp
ON
  dc.day >= asp.start_date
  AND dc.day < asp.end_date
  AND dc.CHAIN = asp.CHAIN 
GROUP BY
  dc.day,
  dc.CHAIN
ORDER BY
  dc.day,
  dc.CHAIN