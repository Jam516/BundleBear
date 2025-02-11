{{ config
(
    materialized = 'table'
)
}}

SELECT name, address, type
FROM (VALUES
('alchemy', '0x4fd9098af9ddcb41da48a1d78f91f1398965addc', 'verifying'), 
('stackup', '0x474ea64bedde53aad1084210bd60eef2989bf80f', 'verifying'),    
('stackup', '0xe93eca6595fe94091dc1af46aac2a8b5d7990770', 'both'),
('stackup', '0x9d6ac51b972544251fcc0f2902e633e3f9bd3f29', 'both'),
('biconomy', '0x000031dd6d9d3a133e663660b959162870d755d4', 'verifying'),
('biconomy', '0x00000f7365ca6c59a2c93719ad53d567ed49c14c', 'verifying'),
('biconomy', '0x00000f79b7faf42eebadba19acc07cd08af44789', 'token'),
('pimlico', '0xa683b47e447de6c8a007d9e294e87b6db333eb18', 'token'),
('pimlico', '0x49ee41bc335fb36be46a17307dcfe536a3494644', 'token'),
('pimlico', '0x939263eafe57038a072cb4edd6b25dd81a8a6c56', 'token'),
('pimlico', '0x0000000000fabfa8079ab313d1d14dcf4d15582a', 'token'),
('pimlico', '0x0000000000fce6614d3c6f679e48c9cdd09aa634', 'token'),
('pimlico', '0x000000000058e13d711bb4706bf822a79c35d8b1', 'token'),
('pimlico', '0x00000000003011eef3f79892ba3d521e5ba5c5c0', 'token'),
('pimlico', '0x984e2abb41a6684e5e213ab61ad4c6c830585df9', 'verifying'),
('pimlico', '0x4df91e173a6cdc74efef6fc72bb5df1e8a8d7582', 'verifying'),
('pimlico', '0x2672a6a67c37c104db47d52baaa721c8eca39421', 'verifying'),
('pimlico', '0xe3dc822d77f8ca7ac74c30b0dffea9fcdcaaa321', 'verifying'),
('pimlico', '0x67f21be69a16c314a0b7da537309b2f3addde031', 'verifying'),
('pimlico', '0x0000000000000039cd5e8ae05257ce51c473ddd1', 'both'),
('pimlico', '0x00000000000000fb866daaa79352cc568a005d96', 'both'),
('candide', '0x769f68e4ba8f53f3092eef34a42a811ab6365b05', 'both'),
('candide', '0x3fe285dcd76bcce4ac92d38a6f2f8e964041e020', 'both'),
('candide', '0xdad1b9318cdf364668cca9bb5a73f6bc83f8a705', 'both'),
('candide', '0x36f4aa64673568782461bf03c75462f8ef0a1b76', 'both'),
('candide', '0x8b1f6cb5d062aa2ce8d581942bbb960420d875ba', 'both'),
('blocto', '0xa312d8d37be746bd09cbd9e9ba2ef16bc7da48ff', 'verifying'),
('circle', '0x7cea357b5ac0639f89f9e378a1f03aa5005c0a25', 'verifying'),
('particle', '0x23b944a93020a9c7c414b1adecdb2fd4cd4e8184', 'verifying'),
('etherspot', '0x7f690e93cecfca5a31e6e1df50a33f6d3059048c', 'verifying'),
('etherspot', '0x26fec24b0d467c9de105217b483931e8f944ff50', 'verifying'),
('etherspot', '0xec2ee24e79c73db13dd9bc782856a5296626b7eb', 'verifying'),
('etherspot', '0x805650ce74561c85baa44a8bd13e19633fd0f79d', 'verifying'),
-- ('safe', '0xe3dc822d77f8ca7ac74c30b0dffea9fcdcaaa321', 'verifying'),
('nani', '0x00000000000009b4ab3f1bc2b029bd7513fbd8ed', 'verifying'),
('nani', '0x00000000000077e2072d61672eb6ec18a136c80a', 'verifying'),
('cometh', '0x6a6b7f6012ee5bef1cdf95df25e5045c7727c739', 'verifying'),
('coinbase', '0xa270ef92c1e11f1c1f95753c2e56801e8125fa83', 'verifying'),
('coinbase', '0x2faeb0760d4230ef2ac21496bb4f0b47d634fd4c', 'both'),
('coinbase', '0xdcbe0c1a00e4cf24ae77c52125e6e6b4f7c6db4e', 'both')
) AS x (name, address, type)