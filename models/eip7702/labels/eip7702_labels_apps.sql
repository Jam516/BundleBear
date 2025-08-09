{{ config
(
    materialized = 'table',
    copy_grants=true
)
}}

SELECT name, address
FROM (VALUES
('Hanafuda', '0xc5bf05cd32a14bffb705fb37a9d218895187376c'),
('$KUNOICHI', '0xa7855ab9147f7a77dcea7c01a69be690e2cd9529'),
('$USDC', '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'),
('Phishing Scam', '0x0791b65d9ab30d03425af32705bf219f58fd4894'),
('Uniswap', '0x66a9893cc07d91d95644aedd05d03f95e1dba8af'),
('$USDC', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'),
('$USDT', '0xdac17f958d2ee523a2206206994597c13d831ec7'),
('Uniswap', '0x6ff5693b99212da76ad316178a184ab56d299b43'),
('Uniswap', '0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24'),
('OKX Swap', '0x9b9efa5efa731ea9bbb0369e91fa17abf249cfd4'),
('Relay', '0xa5f565650890fba1824ee0f21ebbbf660a179934'),
('Relay', '0xbbbfd134e9b44bfb5123898ba36b01de7ab93d98'),
('Relay', '0xf5042e6ffac5a625d4e7848e0b01373d8eb9e222'),
('Layer3 CUBE', '0x1195cf65f83b3a5768f3c496d3a05ad6412c64b7'),
('Relay', '0xaaaaaaae92cc1ceef79a038017889fdd26d23d4d'),
('Opensea', '0x0000000000000068f116a894984e2db1123eb395'),
('$WETH', '0x4200000000000000000000000000000000000006'),
('$BSC-USD', '0x55d398326f99059ff775485246999027b3197955'),
('PancakeSwap', '0x1b81d678ffb9c0263b24a97847620c99d213eb14'),
('PancakeSwap', '0x13f4ea83d0bd40e75c8222255bc855a974568dd4'),
('PancakeSwap', '0xd9c500dff816a1da21a48a732d3498bf09dc9aeb'),
('PancakeSwap', '0x10ed43c718714eb63d5aa57b78b54704e256024e'),
('Festify', '0xae4a3ccb094b1e475ca8b83bcba5508a30ebf1c0')
) AS x (name, address)
