{{ config
(
    materialized = 'table'
)
}}

SELECT name, address
FROM (VALUES
('biconomy', '0x000000f9ee1842bb72f6bbdd75e6d3d4e3e9594c'),    
('safe', '0xa6b71e26c5e0845f74c812102ca7114b6a896ab2'),    
('zerodev', '0xaee9762ce625e0a8f7b184670fb57c37bfe1d0f1'),    
('zerodev', '0x4e4946298614fc299b50c947289f4ad0572cb9ce'),    
('zerodev', '0x19b03a419124b77a7659a0fdab7903c6f40183e5'),    
('blocto', '0xf7ccfaee69cd8a0b3a62c2a0f35f95cc7e588183'),
('banana', '0x8e5ffc77d0906618a8ed73db34f92ea0251b327b'),
('fun.xyz', '0xda2ba73e0dd91d8bbb72dc10655b691295bdf985'),
('candide', '0xb73eb505abc30d0e7e15b73a492863235b3f4309')
) AS x (name, address)