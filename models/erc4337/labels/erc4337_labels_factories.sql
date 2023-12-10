{{ config
(
    materialized = 'table'
)
}}

SELECT name, address
FROM (VALUES
('biconomy', '0x000000f9ee1842bb72f6bbdd75e6d3d4e3e9594c'), 
('biconomy', '0x000000a56aaca3e9a4c479ea6b6cd0dbcb6634f5'),    
('safe', '0xa6b71e26c5e0845f74c812102ca7114b6a896ab2'),    
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
('etherspot', '0x7f6d8f107fe8551160bd5351d5f1514a6ad5d40e')
) AS x (name, address)