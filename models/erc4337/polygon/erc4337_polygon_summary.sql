{{ config
(
    materialized = 'table'
)
}}

WITH deployments AS (
    SELECT COUNT(*) as NUM_DEPLOYMENTS
    FROM {{ ref('erc4337_polygon_account_deployments') }}
)

, userops AS (
    SELECT COUNT(*) as NUM_USEROPS
    FROM {{ ref('erc4337_polygon_userops') }}
)

, txns AS (
    SELECT COUNT(*) as NUM_TXNS
    FROM {{ ref('erc4337_polygon_entrypoint_transactions') }}
)

, paymaster_spend AS (
    SELECT 
    ROUND(SUM(ACTUALGASCOST_USD)) AS GAS_SPENT
    FROM {{ ref('erc4337_polygon_userops') }}
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
)

SELECT * FROM deployments, userops, txns, paymaster_spend