{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT name, address
FROM (VALUES
('WhiteBit Account', '0x000000f9ee1842bb72f6bbdd75e6d3d4e3e9594c'),
('Metamask 7702Delegator', '0x000000a56aaca3e9a4c479ea6b6cd0dbcb6634f5'),
('Ambire 7702Account', '0x000000001d1d5004a02bafab9de2d6ce5b7b13de'),
('Simple 7702Account', '0x000000c3a93d2c5e02cb053ac675665b1c4217f9'),
('OKX 7702Wallet', '0x000000226cada0d8b36034f5d5c06855f59f6f3a')
) AS x (name, address)
