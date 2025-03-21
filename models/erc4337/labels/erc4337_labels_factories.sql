{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT name, address
FROM (VALUES
('biconomy', '0x000000f9ee1842bb72f6bbdd75e6d3d4e3e9594c'),
('biconomy', '0x000000a56aaca3e9a4c479ea6b6cd0dbcb6634f5'),
('zerodev_kernel', '0xaee9762ce625e0a8f7b184670fb57c37bfe1d0f1'),
('zerodev_kernel', '0x4e4946298614fc299b50c947289f4ad0572cb9ce'),
('zerodev_kernel', '0x19b03a419124b77a7659a0fdab7903c6f40183e5'),
('zerodev_kernel', '0x5de4839a76cf55d0c90e2061ef4386d962e15ae3'),
('zerodev_kernel', '0x33ddf684dcc6937ffe59d8405aa80c41fb518c5c'),
('blocto', '0xf7ccfaee69cd8a0b3a62c2a0f35f95cc7e588183'),
('banana', '0x8e5ffc77d0906618a8ed73db34f92ea0251b327b'),
('fun.xyz', '0xda2ba73e0dd91d8bbb72dc10655b691295bdf985'),
('candide', '0xb73eb505abc30d0e7e15b73a492863235b3f4309'),
('simpleaccount', '0x15ba39375ee2ab563e8873c8390be6f2e2f50232'),
('simpleaccount', '0x9406cc6185a346906296840746125a0e44976454'),
('alchemy_lightaccount', '0x000000893a26168158fbeadd9335be5bc96592e2'),
('alchemy_lightaccount', '0x00004ec70002a32400f8ae005a26081065620d20'),
('alchemy_lightaccount', '0xc1b2fc4197c9187853243e6e4eb5a4af8879a1c0'),
('alchemy_lightaccount', '0x0000000000400cdfef5e2714e63d8040b700bc24'),
('alchemy_lightaccount', '0x8e8e658e22b12ada97b402ff0b044d6a325013c7'),
('alchemy_modularaccount', '0x000000e92d78d90000007f0082006fda09bd5f11'),
('alchemy_modularaccount', '0x000000000019d2ee9f2729a65afe20bb0020aefc'),
('alchemy_modularaccount', '0xd2c27f9ee8e4355f71915ffd5568cb3433b6823d'),
('etherspot', '0x7f6d8f107fe8551160bd5351d5f1514a6ad5d40e'),
('thirdweb_default', '0x85e23b94e7f5e9cc1ff78bce78cfb15b81f0df00'),
('thirdweb_managedaccount', '0x463effb51873c7720c810ac7fb2e145ec2f8cc60'),
('thirdweb_managedaccount', '0xa0b9ebd2cc138e0748c69baf66df2e01c57521ec'),
('thirdweb_managedaccount', '0x0c4176d4a4e2bb32ede3bfab928fd26ad8d15749'),
('thirdweb_managedaccount', '0x1d126c5366aaaebe53fed61242553923284e2f5b'),
('polynomial', '0xb43c0899eccf98bc7a0f3e2c2a211d6fc4f9b3fe'),
('circle', '0xfef1c57185393f456eaeca363a0d3c12cd8df07b'),
('circle', '0x1a1f5310ed7ff0b84cef7e6d7d0f94cc16d23013'),
('circle', '0xa233b124d7b9cff2d38cb62319e1a3f79144b490'),
('circle', '0xf61023061ed45fa9eac4d2670649ce1fd37ce536'),
('circle', '0x0000000df7e6c9dc387cafc5ecbfa6c3a6179add'),
('nani', '0x000000000000dd366cc2e4432bb998e41dfd47c7'),
('nani', '0x0000000000009f1e546fc4a8f68eb98031846cb8'),
('zerodev_kernel', '0xd703aae79538628d27099b8c4f621be4ccd142d5'),
('coinbase_smart_wallet', '0x0ba5ed0c6aa8c49038f819e587e2633c4a9f428a'),
('lumx', '0x2e1c14daadefc4a85eaec81dacba27cd455a0b66'),
('Send', '0x008c9561857b6555584d20ac55110335759aa2c2'),
('splits_smart_vault', '0x8E6Af8Ed94E87B4402D0272C5D6b0D47F0483e7C')
) AS x (name, address)
