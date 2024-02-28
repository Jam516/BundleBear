{{ config
(
    materialized = 'table'
)
}}

with
address_table as (  
    select ACCOUNT_ADDRESS as address_filter
    from BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ACCOUNT_DEPLOYMENTS
)

select
    '2023-01-01 00:00:00.000'::timestamp as date,
    balances.address,
    balances.usd_balance,
    balances.balance,
    balances.token_address,
    balances.token_symbol
from ETHEREUM.ASSETS.ETH_AND_ERC20_BALANCES balances 
inner join address_table 
    on address = address_filter
    AND block_timestamp <= TO_DATE('2023-04-01', 'YYYY-MM-DD')
    and balances.usd_balance is not null
    QUALIFY ROW_NUMBER() OVER(PARTITION BY balances.address, balances.token_address ORDER BY balances.block_timestamp DESC) = 1