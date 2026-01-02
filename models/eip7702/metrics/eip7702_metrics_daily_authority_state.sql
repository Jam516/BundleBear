{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['day', 'chain']
)
}}
WITH date_spine AS (
{% if is_incremental() %}
  SELECT DATEADD(DAY, -SEQ4(), DATEADD(DAY, -1, CURRENT_DATE())) AS day
  FROM TABLE(GENERATOR(ROWCOUNT => 4))
{% else %}
  SELECT DATEADD(DAY, seq, DATE('2025-03-20')) AS day
  FROM (
    SELECT SEQ4() AS seq
    FROM TABLE(GENERATOR(ROWCOUNT => 2000))
  )
  WHERE seq <= DATEDIFF('DAY', DATE('2025-03-20'), DATEADD(DAY, -1, CURRENT_DATE()))
{% endif %}
),

chain_metrics AS (
SELECT
  d.day,
  s.CHAIN,
  COUNT(DISTINCT CASE WHEN s.is_smart_wallet THEN s.AUTHORITY END) AS live_smart_wallets,
  COUNT(DISTINCT CASE WHEN s.is_smart_wallet THEN s.AUTHORIZED_CONTRACT END) AS live_authorized_contracts
FROM
  date_spine d
INNER JOIN
  {{ ref('eip7702_state_base') }} s
ON
  d.day >= s.start_day
  AND (s.end_day IS NULL OR d.day <= s.end_day)
GROUP BY 1, 2
),

cross_chain_metrics AS (
  SELECT
    d.day,
    'cross-chain' AS CHAIN,
    COUNT(DISTINCT CASE WHEN s.is_smart_wallet THEN s.AUTHORITY END) AS live_smart_wallets,
    COUNT(DISTINCT CASE WHEN s.is_smart_wallet THEN s.AUTHORIZED_CONTRACT END) AS live_authorized_contracts
FROM
  date_spine d
INNER JOIN
  {{ ref('eip7702_state_base') }} s
ON
  d.day >= s.start_day
  AND (s.end_day IS NULL OR d.day <= s.end_day)
GROUP BY 1, 2
)

SELECT * FROM chain_metrics
UNION ALL
SELECT * FROM cross_chain_metrics
