{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT name, address
FROM (VALUES
('WhiteBit Account', '0xcda3577ca7ef65f6b7201e9bd80375f5628d15f7'),
('Metamask Delegator', '0x63c0c19a282a1b52b07dd5a65b58948a07dae32b'),
('Ambire Account', '0x5a7fc11397e9a8ad41bf10bf13f22b0a63f96f6d'),
('Simple 7702Account', '0xe6cae83bde06e4c305530e199d7217f42808555b'),
('OKX 7702Wallet', '0x80296ff8d1ed46f8e3c7992664d13b833504c2bb'),
('Biconomy Nexus', '0x000000004f43c49e93c970e84001853a70923b03 '),
('Trustwallet Biz', '0xd2e28229f6f2c235e57de2ebc727025a1d0530fb ')
) AS x (name, address)
