{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

-- Step 1: Find the latest state for each authority on each day
WITH authority_daily_states AS (
  SELECT
    AUTHORITY,
    AUTHORIZED_CONTRACT,
    DATE_TRUNC('DAY', BLOCK_TIME) AS state_date,
    ROW_NUMBER() OVER (
      PARTITION BY AUTHORITY, DATE_TRUNC('DAY', BLOCK_TIME) 
      ORDER BY NONCE DESC, BLOCK_TIME DESC
    ) AS daily_rank
  FROM 
    {{ ref('eip7702_all_authorizations') }}
  WHERE CHAIN_ID = 0
),

-- Step 2: Keep only the latest state for each day
latest_daily_states AS (
  SELECT
    AUTHORITY,
    AUTHORIZED_CONTRACT,
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
    state_date AS start_date,
    LEAD(state_date, 1, DATEADD(DAY, 1, CURRENT_DATE())) OVER (
      PARTITION BY AUTHORITY 
      ORDER BY state_date
    ) AS end_date
  FROM 
    latest_daily_states
),

-- Step 4: Generate date series using a date spine table approach
date_spine AS (
  SELECT DATEADD(DAY, SEQ4(), (SELECT MIN(state_date) FROM latest_daily_states)) AS day
  FROM TABLE(GENERATOR(ROWCOUNT => 1000))  -- Using a fixed number that covers your date range
  QUALIFY day <= CURRENT_DATE()
)

-- Step 5: Calculate daily metrics
SELECT
  d.day,
  'cross-chain' AS chain,
  COUNT(DISTINCT CASE WHEN asp.AUTHORIZED_CONTRACT != '0x0000000000000000000000000000000000000000' THEN asp.AUTHORITY END) AS live_smart_wallets,
  COUNT(DISTINCT CASE WHEN asp.AUTHORIZED_CONTRACT != '0x0000000000000000000000000000000000000000' THEN asp.AUTHORIZED_CONTRACT END) AS live_authorized_contracts
FROM 
  date_spine d
LEFT JOIN
  authority_state_periods asp
ON
  d.day >= asp.start_date
  AND d.day < asp.end_date
GROUP BY
  1,2
ORDER BY
  1