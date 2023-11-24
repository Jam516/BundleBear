{{ config
(
    materialized = 'table'
)
}}

SELECT name, address
FROM (VALUES
('cyberconnect', '0xaee9762ce625e0a8f7b184670fb57c37bfe1d0f1'),       
('patch', '0x33ddf684dcc6937ffe59d8405aa80c41fb518c5c')   
) AS x (name, address)