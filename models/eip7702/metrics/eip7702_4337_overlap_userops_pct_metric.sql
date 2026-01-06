{{ config
(
    materialized = 'incremental',
    copy_grants=true,
    unique_key = ['date', 'timeframe', 'chain']
)
}}
WITH day_all AS (
    SELECT
        date_trunc('day', BLOCK_TIME) AS DATE,
        CHAIN,
        COUNT(OP_HASH) AS NUM_USEROPS_ALL
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE
        {% if is_incremental() %}
        BLOCK_TIME >= DATEADD(DAY, -3, CURRENT_DATE())
        {% else %}
        BLOCK_TIME >= DATE('2025-05-05')
        {% endif %}
        AND BLOCK_TIME < CURRENT_DATE()
    GROUP BY 1,2
),

day_eip AS (
    SELECT
        date_trunc('day', BLOCK_TIME) AS DATE,
        CHAIN,
        COUNT(OP_HASH) AS NUM_USEROPS_EIP
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
    WHERE TYPE = 'erc-4337 userop'
        AND
        {% if is_incremental() %}
        BLOCK_TIME >= DATEADD(DAY, -3, CURRENT_DATE())
        {% else %}
        BLOCK_TIME >= DATE('2025-05-05')
        {% endif %}
        AND BLOCK_TIME < CURRENT_DATE()
    GROUP BY 1,2
),

day_metrics AS (
    SELECT
        TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
        a.CHAIN,
        'day' AS TIMEFRAME,
        100 * COALESCE(e.NUM_USEROPS_EIP, 0) / NULLIF(a.NUM_USEROPS_ALL, 0)
            AS PCT_EIP7702_USEROPS
    FROM day_all a
    LEFT JOIN day_eip e
        ON a.DATE = e.DATE
        AND a.CHAIN = e.CHAIN

    UNION ALL

    SELECT
        TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
        'all' AS CHAIN,
        'day' AS TIMEFRAME,
        100 * COALESCE(SUM(e.NUM_USEROPS_EIP), 0) / NULLIF(SUM(a.NUM_USEROPS_ALL), 0)
            AS PCT_EIP7702_USEROPS
    FROM day_all a
    LEFT JOIN day_eip e
        ON a.DATE = e.DATE
        AND a.CHAIN = e.CHAIN
    GROUP BY 1,2,3
),

week_all AS (
    SELECT
        date_trunc('week', BLOCK_TIME) AS DATE,
        CHAIN,
        COUNT(OP_HASH) AS NUM_USEROPS_ALL
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE
        {% if is_incremental() %}
        BLOCK_TIME >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '1 week'
        {% else %}
        BLOCK_TIME >= DATE('2025-05-05')
        {% endif %}
        AND BLOCK_TIME < DATE_TRUNC('week', CURRENT_DATE())
    GROUP BY 1,2
),

week_eip AS (
    SELECT
        date_trunc('week', BLOCK_TIME) AS DATE,
        CHAIN,
        COUNT(OP_HASH) AS NUM_USEROPS_EIP
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
    WHERE TYPE = 'erc-4337 userop'
        AND
        {% if is_incremental() %}
        BLOCK_TIME >= DATE_TRUNC('week', CURRENT_DATE()) - INTERVAL '1 week'
        {% else %}
        BLOCK_TIME >= DATE('2025-05-05')
        {% endif %}
        AND BLOCK_TIME < DATE_TRUNC('week', CURRENT_DATE())
    GROUP BY 1,2
),

week_metrics AS (
    SELECT
        TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
        a.CHAIN,
        'week' AS TIMEFRAME,
        100 * COALESCE(e.NUM_USEROPS_EIP, 0) / NULLIF(a.NUM_USEROPS_ALL, 0)
            AS PCT_EIP7702_USEROPS
    FROM week_all a
    LEFT JOIN week_eip e
        ON a.DATE = e.DATE
        AND a.CHAIN = e.CHAIN

    UNION ALL

    SELECT
        TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
        'all' AS CHAIN,
        'week' AS TIMEFRAME,
        100 * COALESCE(SUM(e.NUM_USEROPS_EIP), 0) / NULLIF(SUM(a.NUM_USEROPS_ALL), 0)
            AS PCT_EIP7702_USEROPS
    FROM week_all a
    LEFT JOIN week_eip e
        ON a.DATE = e.DATE
        AND a.CHAIN = e.CHAIN
    GROUP BY 1,2,3
),

month_all AS (
    SELECT
        date_trunc('month', BLOCK_TIME) AS DATE,
        CHAIN,
        COUNT(OP_HASH) AS NUM_USEROPS_ALL
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE
        {% if is_incremental() %}
        BLOCK_TIME >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '1 month'
        {% else %}
        BLOCK_TIME >= DATE('2025-05-05')
        {% endif %}
        AND BLOCK_TIME < DATE_TRUNC('month', CURRENT_DATE())
    GROUP BY 1,2
),

month_eip AS (
    SELECT
        date_trunc('month', BLOCK_TIME) AS DATE,
        CHAIN,
        COUNT(OP_HASH) AS NUM_USEROPS_EIP
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ACTIONS
    WHERE TYPE = 'erc-4337 userop'
        AND
        {% if is_incremental() %}
        BLOCK_TIME >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '1 month'
        {% else %}
        BLOCK_TIME >= DATE('2025-05-05')
        {% endif %}
        AND BLOCK_TIME < DATE_TRUNC('month', CURRENT_DATE())
    GROUP BY 1,2
),

month_metrics AS (
    SELECT
        TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
        a.CHAIN,
        'month' AS TIMEFRAME,
        100 * COALESCE(e.NUM_USEROPS_EIP, 0) / NULLIF(a.NUM_USEROPS_ALL, 0)
            AS PCT_EIP7702_USEROPS
    FROM month_all a
    LEFT JOIN month_eip e
        ON a.DATE = e.DATE
        AND a.CHAIN = e.CHAIN

    UNION ALL

    SELECT
        TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
        'all' AS CHAIN,
        'month' AS TIMEFRAME,
        100 * COALESCE(SUM(e.NUM_USEROPS_EIP), 0) / NULLIF(SUM(a.NUM_USEROPS_ALL), 0)
            AS PCT_EIP7702_USEROPS
    FROM month_all a
    LEFT JOIN month_eip e
        ON a.DATE = e.DATE
        AND a.CHAIN = e.CHAIN
    GROUP BY 1,2,3
)

SELECT
    DATE,
    TIMEFRAME,
    CHAIN,
    PCT_EIP7702_USEROPS
FROM day_metrics

UNION ALL
SELECT
    DATE,
    TIMEFRAME,
    CHAIN,
    PCT_EIP7702_USEROPS
FROM week_metrics

UNION ALL
SELECT
    DATE,
    TIMEFRAME,
    CHAIN,
    PCT_EIP7702_USEROPS
FROM month_metrics
