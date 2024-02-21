{{ config
(
    materialized = 'incremental',
    unique_key = ['tx_hash', 'log_index']
)
}}

select
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as address,
'ethereum' as blockchain,
TRANSACTION_HASH as tx_hash,
LOG_INDEX
from ETHEREUM.RAW.LOGS
where ADDRESS = '0x3adae9699029ab2953f607ae1f62372681d35978' --AvoFactoryProxy
and topic0 = '0x0e2d0feff5e8ebc6a92c82e612b2eb241d5fc1f0c7b54ebd2637384f8a75d324' --AvoSafeDeployed
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2023-01-01'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
union all select
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as address,
'ethereum' as blockchain,
TRANSACTION_HASH as tx_hash,
LOG_INDEX
from ETHEREUM.RAW.LOGS
where ADDRESS = '0xe981E50c7c47F0Df8826B5ce3F533f5E4440e687' --AvoFactoryProxy
and topic0 = '0xb2e6130f590922fd6e9fbd3b5f2c7aab70d0c0a10ff31b9ee72231511e038a8e' --AvoSafeDeployed
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2023-08-24'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}

union all select
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as address,
'polygon' as blockchain,
TRANSACTION_HASH as tx_hash,
LOG_INDEX
from POLYGON.RAW.LOGS
where ADDRESS = '0x3adae9699029ab2953f607ae1f62372681d35978' --AvoFactoryProxy
and topic0 = '0x0e2d0feff5e8ebc6a92c82e612b2eb241d5fc1f0c7b54ebd2637384f8a75d324' --AvoSafeDeployed
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2023-01-01'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
union all select
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as address,
'polygon' as blockchain,
TRANSACTION_HASH as tx_hash,
LOG_INDEX
from POLYGON.RAW.LOGS
where ADDRESS = '0xe981E50c7c47F0Df8826B5ce3F533f5E4440e687' --AvoFactoryProxy
and topic0 = '0xb2e6130f590922fd6e9fbd3b5f2c7aab70d0c0a10ff31b9ee72231511e038a8e' --AvoSafeDeployed
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2023-08-24'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}

union all select
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as address,
'optimism' as blockchain,
TRANSACTION_HASH as tx_hash,
LOG_INDEX
from OPTIMISM.RAW.LOGS
where ADDRESS = '0x3adae9699029ab2953f607ae1f62372681d35978' --AvoFactoryProxy
and topic0 = '0x0e2d0feff5e8ebc6a92c82e612b2eb241d5fc1f0c7b54ebd2637384f8a75d324' --AvoSafeDeployed
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2023-01-01'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
union all select
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as address,
'optimism' as blockchain,
TRANSACTION_HASH as tx_hash,
LOG_INDEX
from OPTIMISM.RAW.LOGS
where ADDRESS = '0xe981E50c7c47F0Df8826B5ce3F533f5E4440e687' --AvoFactoryProxy
and topic0 = '0xb2e6130f590922fd6e9fbd3b5f2c7aab70d0c0a10ff31b9ee72231511e038a8e' --AvoSafeDeployed
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2023-08-24'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}

union all select
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as address,
'arbitrum' as blockchain,
TRANSACTION_HASH as tx_hash,
LOG_INDEX
from ARBITRUM.RAW.LOGS
where ADDRESS = '0x3adae9699029ab2953f607ae1f62372681d35978' --AvoFactoryProxy
and topic0 = '0x0e2d0feff5e8ebc6a92c82e612b2eb241d5fc1f0c7b54ebd2637384f8a75d324' --AvoSafeDeployed
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2023-01-01'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}
union all select
CONCAT('0x', SUBSTRING(TOPIC2, 27, 40)) as address,
'arbitrum' as blockchain,
TRANSACTION_HASH as tx_hash,
LOG_INDEX
from ARBITRUM.RAW.LOGS
where ADDRESS = '0xe981E50c7c47F0Df8826B5ce3F533f5E4440e687' --AvoFactoryProxy
and topic0 = '0xb2e6130f590922fd6e9fbd3b5f2c7aab70d0c0a10ff31b9ee72231511e038a8e' --AvoSafeDeployed
{% if not is_incremental() %}
and BLOCK_TIMESTAMP > '2023-08-24'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}