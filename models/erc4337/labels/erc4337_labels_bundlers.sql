{{ config
(
    materialized = 'table'
)
}}

SELECT name, address
FROM (VALUES
('stackup', '0x25df024637d4e56c1ae9563987bf3e92c9f534c0'),    
('stackup', '0x6892bef4ae1b5cb33f9a175ab822518c9025fc3c'),    
('stackup', '0xfd72ae8ff5cc18849d83f13a252a0d8fd99eb0ac'),    
('stackup', '0x9831d6f598729bf41055a0af96396cea91eab18b'),    
('stackup', '0x20e9695f25413f14e5807b530D0698bd4F155074'),    
('stackup', '0x81c99a7eb142f4e2d8f0681b777d14f65d7a7e53'),
('stackup', '0x9c98b1528c26cf36e78527308c1b21d89baed700')
) AS x (name, address)