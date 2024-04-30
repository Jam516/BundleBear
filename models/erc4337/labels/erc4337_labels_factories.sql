{{ config
(
    materialized = 'table'
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
('alchemy_lightaccount', '0x000000e92d78d90000007f0082006fda09bd5f11'),
('etherspot', '0x7f6d8f107fe8551160bd5351d5f1514a6ad5d40e'),
('thirdweb', '0x872f64d0510c8c470188bc29487ea7ab79faa518'),
('thirdweb', '0xebdacbaf7e6f2521250da8713ecacbaf10ccbe8a'),
('thirdweb', '0xa0b9ebd2cc138e0748c69baf66df2e01c57521ec'),
('polynomial', '0xb43c0899eccf98bc7a0f3e2c2a211d6fc4f9b3fe'),
('circle', '0xfef1c57185393f456eaeca363a0d3c12cd8df07b'),
('nani', '0x000000000000dd366cc2e4432bb998e41dfd47c7'),
('zerodev_kernel', '0xd703aae79538628d27099b8c4f621be4ccd142d5')
) AS x (name, address)