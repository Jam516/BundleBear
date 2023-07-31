{{ config
(
    materialized = 'table'
)
}}

SELECT name, address
FROM (VALUES
('stackup', '0x474ea64bedde53aad1084210bd60eef2989bf80f'),    
('stackup', '0xe93eca6595fe94091dc1af46aac2a8b5d7990770')
) AS x (name, address)