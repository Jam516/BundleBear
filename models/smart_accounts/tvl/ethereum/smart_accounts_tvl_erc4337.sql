{{ config
(
    materialized = 'table'
)
}}

with
address_table as (  -- CTE of address input
    select ACCOUNT_ADDRESS as address_filter
    from BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ACCOUNT_DEPLOYMENTS
),
quarterly_balances as ( -- Select the last balance of each token for each wallet by time window
    select * from BUNDLEBEAR.DBT_KOFI.SMART_ACCOUNTS_STARTING_BALANCE_ERC4337
    union all
    select
        date_trunc(quarter, balances.block_timestamp) as date,
        balances.address,
        balances.usd_balance,
        balances.balance,
        balances.token_address,
        balances.token_symbol
    from ETHEREUM.ASSETS.ETH_AND_ERC20_BALANCES balances 
    inner join address_table 
        on address = address_filter
        AND block_timestamp > '2023-04-01'
        and balances.usd_balance is not null
    qualify row_number() over (partition by address, token_address, date order by block_number desc) = 1
),
hoders_dates as ( -- Generate timestamp x address x tokens 
    select date_trunc(quarter, timestamp) as date, address, token_address, count(1) as count_all
    from ethereum.raw.blocks, quarterly_balances
    where timestamp >= (select min(date) from quarterly_balances)
    group by all
)
,
final_balances as (
  select
t1.date as balance_quarter,
t1.address,
lag(t2.token_address) ignore nulls over (
    partition by t1.address, t1.token_address order by t1.date
) as token_address,
lag(t2.token_symbol) ignore nulls over (partition by t1.address, t1.token_address order by t1.date) as token_symbol,
lag(t2.usd_balance) ignore nulls over (partition by t1.address, t1.token_address order by t1.date) as usd_balance,
lag(t2.balance) ignore nulls over (partition by t1.address, t1.token_address order by t1.date) as balance
from hoders_dates t1
left join quarterly_balances t2 on t1.date = t2.date and t1.address = t2.address and t1.token_address = t2.token_address
where 1 = 1
)
select *
from final_balances
where balance > 0 