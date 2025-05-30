{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT name, address
FROM (VALUES
('WhiteBit Account', '0xcda3577ca7ef65f6b7201e9bd80375f5628d15f7'),
('WhiteBit Account', '0x79cf9e04ad9aeb210768c22c228673aed6cd24c4'),
('Metamask Delegator', '0x63c0c19a282a1b52b07dd5a65b58948a07dae32b'),
('Ambire Account', '0x5a7fc11397e9a8ad41bf10bf13f22b0a63f96f6d'),
('Simple 7702Account', '0xe6cae83bde06e4c305530e199d7217f42808555b'),
('OKX 7702Wallet', '0x80296ff8d1ed46f8e3c7992664d13b833504c2bb'),
('Biconomy Nexus', '0x000000004f43c49e93c970e84001853a70923b03'),
('Trustwallet Biz', '0xd2e28229f6f2c235e57de2ebc727025a1d0530fb'),
('Uniswap', '0x0c338ca25585035142a9a0a1eeeba267256f281f'),
('Uniswap', '0x458f5a9f47a01bea5d7a32662660559d9ed3312c'),
('Uniswap', '0x000000009b1d0af20d8c6d0a44e162d11f9b8f00'),
('Alchemy', '0x69007702764179f14f51cdce752f4f775d74e139'),
('Thirdweb', '0xbac7e770af15d130cd72838ff386f14fbf3e9a3d'),
('Zerodev', '0xd6cedde84be40893d153be9d467cd6ad37875b28'),
('Porto', '0x664ab8c20b629422f5398e58ff8989e68b26a4e6'),
('Coinbase Wallet', '0x7702cb554e6bfb442cb743a7df23154544a7176c'),
('Fireblocks', '0x0000fb7702036ff9f76044a501ac1aa74cbab16b'),
('CrimeEnjoyor', '0x349c41a8e164a243203605dbd07889d201174d77'),
('CrimeEnjoyor', '0x5c0935ac050e939565c3e42a6882074ebb3eabda'),
('CrimeEnjoyor', '0x6ae436a71612c5875c4d322ee112bf34e64cd6e1'),
('CrimeEnjoyor', '0xf903dd08547de6601ca1d0a880d0d9912d762d5e'),
('CrimeEnjoyor', '0xe35ac76765d80e60b3fdec2eb146c74145690387'),
('CrimeEnjoyor', '0xb847f107513522af770ee0aad8da0319e6da32b3'),
('CrimeEnjoyor', '0x3aae056497edd0a3df5f9405e2f1bec7a5f56dd5'),
('CrimeEnjoyor', '0xe38e81a06ada5c4515a1fc8266ae470da63c00b4'),
('CrimeEnjoyor', '0x3549c7f6a9d712fd3007efc1b85e0c4acca5c211'),
('CrimeEnjoyor', '0x710fad1041f0ee79916bb1a6adef662303bb8b6e'),
('CrimeEnjoyor', '0x1107396baebd1da108fdb2691d08a3b3f831b4d6'),
('CrimeEnjoyor', '0x84d05511614272694d3a9cebe896514dbde51f40'),
('CrimeEnjoyor', '0xc6cb2c4d7c277bbd774fd9a9e485c1da0a460ade'),
('CrimeEnjoyor', '0x15c432e31d073c85f51b31016ff70f0874a5bab3'),
('CrimeEnjoyor', '0x5d595731fbdba356ae71b65f6f014749a4eb969a'),
('CrimeEnjoyor', '0xfdee40030641b66a6af7a53eeedd4740fedb761c'),
('CrimeEnjoyor', '0xc99f40a9c952ce7e29e2f8b6c7461cdb60c1b54a'),
('CrimeEnjoyor', '0x863cf72e70c2e6ae47078ea8b4e135a4d350572f'),
('CrimeEnjoyor', '0xe6827c2a2167bffbf84fa02d94a4d25668434313'),
('CrimeEnjoyor', '0x89383882fc2d0cd4d7952a3267a3b6dae967e704'),
('CrimeEnjoyor', '0x9ea61f15cdbaf5d2039771381fa2adcfb1b76321'),
('CrimeEnjoyor', '0x633288b20f63d9f6f71037d6cd4a5090436134f2'),
('CrimeEnjoyor', '0xf3df663c15710b98f83e48c010b9cd731ae345ca'),
('CrimeEnjoyor', '0x3220bf967f84160905e4d4326f7dbcd0a2f5a5bf'),
('CrimeEnjoyor', '0x1ee8e3b6ca95606e21be70cff6a0bd24c134b96f'),
('CrimeEnjoyor', '0xcefd060da801a3f004d6b307f4cab943d1c9b45b')
) AS x (name, address)
