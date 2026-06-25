--
-- Seed data for the Citus multi-tenant tutorial schema.
-- All IDs are explicit (OVERRIDING SYSTEM VALUE) so that foreign-key
-- references are deterministic and sequences on the target are set
-- correctly by pgcopydb.
--

-- 5 companies
insert into companies (id, name, image_url, created_at, updated_at)
    overriding system value
values
    (1, 'Acme Corp',     'https://example.com/acme.png',     '2024-01-01', '2024-01-01'),
    (2, 'Globex',        'https://example.com/globex.png',   '2024-01-01', '2024-01-01'),
    (3, 'Initech',       'https://example.com/initech.png',  '2024-01-01', '2024-01-01'),
    (4, 'Umbrella Corp', 'https://example.com/umbrella.png', '2024-01-01', '2024-01-01'),
    (5, 'Hooli',         'https://example.com/hooli.png',    '2024-01-01', '2024-01-01');

-- 2 campaigns per company (10 total)
insert into campaigns (company_id, id, name, cost_model, state, monthly_budget, created_at, updated_at)
    overriding system value
values
    (1, 1,  'Spring Sale',      'cpc', 'active', 10000, '2024-01-01', '2024-01-01'),
    (1, 2,  'Summer Push',      'cpm', 'paused',  5000, '2024-01-01', '2024-01-01'),
    (2, 3,  'Brand Awareness',  'cpc', 'active', 20000, '2024-01-01', '2024-01-01'),
    (2, 4,  'Retargeting',      'cpc', 'active',  8000, '2024-01-01', '2024-01-01'),
    (3, 5,  'Launch Campaign',  'cpm', 'active', 15000, '2024-01-01', '2024-01-01'),
    (3, 6,  'Product Refresh',  'cpc', 'paused',  3000, '2024-01-01', '2024-01-01'),
    (4, 7,  'Global Outreach',  'cpm', 'active', 50000, '2024-01-01', '2024-01-01'),
    (4, 8,  'Local Promo',      'cpc', 'active',  2000, '2024-01-01', '2024-01-01'),
    (5, 9,  'Tech Blitz',       'cpc', 'active', 30000, '2024-01-01', '2024-01-01'),
    (5, 10, 'Viral Drive',      'cpm', 'paused', 12000, '2024-01-01', '2024-01-01');

-- 2 ads per campaign (20 total)
insert into ads (company_id, id, campaign_id, name, impressions_count, clicks_count, created_at, updated_at)
    overriding system value
values
    (1,  1,  1,  'Acme Ad A',      1200, 45, '2024-01-01', '2024-01-01'),
    (1,  2,  1,  'Acme Ad B',       800, 30, '2024-01-01', '2024-01-01'),
    (1,  3,  2,  'Acme Summer A',   500, 10, '2024-01-01', '2024-01-01'),
    (1,  4,  2,  'Acme Summer B',   300,  5, '2024-01-01', '2024-01-01'),
    (2,  5,  3,  'Globex Brand A', 2000, 90, '2024-01-01', '2024-01-01'),
    (2,  6,  3,  'Globex Brand B', 1800, 75, '2024-01-01', '2024-01-01'),
    (2,  7,  4,  'Globex Retarget A', 900, 40, '2024-01-01', '2024-01-01'),
    (2,  8,  4,  'Globex Retarget B', 600, 22, '2024-01-01', '2024-01-01'),
    (3,  9,  5,  'Initech Launch A', 3000, 120, '2024-01-01', '2024-01-01'),
    (3, 10,  5,  'Initech Launch B', 2500, 100, '2024-01-01', '2024-01-01'),
    (3, 11,  6,  'Initech Refresh A', 400, 15, '2024-01-01', '2024-01-01'),
    (3, 12,  6,  'Initech Refresh B', 200,  8, '2024-01-01', '2024-01-01'),
    (4, 13,  7,  'Umbrella Global A', 5000, 200, '2024-01-01', '2024-01-01'),
    (4, 14,  7,  'Umbrella Global B', 4500, 180, '2024-01-01', '2024-01-01'),
    (4, 15,  8,  'Umbrella Local A',   700, 28, '2024-01-01', '2024-01-01'),
    (4, 16,  8,  'Umbrella Local B',   350, 12, '2024-01-01', '2024-01-01'),
    (5, 17,  9,  'Hooli Blitz A',    4000, 160, '2024-01-01', '2024-01-01'),
    (5, 18,  9,  'Hooli Blitz B',    3500, 140, '2024-01-01', '2024-01-01'),
    (5, 19, 10,  'Hooli Viral A',    1500, 60, '2024-01-01', '2024-01-01'),
    (5, 20, 10,  'Hooli Viral B',    1200, 48, '2024-01-01', '2024-01-01');

-- 2 clicks per ad for company 1 (ad IDs 1–4)
insert into clicks (company_id, id, ad_id, clicked_at, site_url, cost_per_click_usd, user_ip, user_data)
    overriding system value
values
    (1, 1, 1, '2024-02-01 10:00:00', 'https://news.example.com', 0.25, '203.0.113.5',  '{"browser":"Chrome"}'),
    (1, 2, 1, '2024-02-01 10:05:00', 'https://blog.example.com', 0.18, '203.0.113.6',  '{"browser":"Firefox"}'),
    (1, 3, 2, '2024-02-02 11:00:00', 'https://shop.example.com', 0.30, '203.0.113.7',  '{"browser":"Safari"}'),
    (1, 4, 2, '2024-02-02 11:10:00', 'https://news.example.com', 0.22, '203.0.113.8',  '{"browser":"Edge"}'),
    (1, 5, 3, '2024-02-03 09:00:00', 'https://tech.example.com', 0.15, '203.0.113.9',  '{"browser":"Chrome"}'),
    (1, 6, 4, '2024-02-03 09:30:00', 'https://blog.example.com', 0.20, '203.0.113.10', '{"browser":"Firefox"}');

-- 2 impressions per ad for company 2 (ad IDs 5–8)
insert into impressions (company_id, id, ad_id, seen_at, site_url, cost_per_impression_usd, user_ip, user_data)
    overriding system value
values
    (2, 1, 5, '2024-02-01 08:00:00', 'https://news.example.com', 0.002, '198.51.100.1', '{"os":"Linux"}'),
    (2, 2, 5, '2024-02-01 08:05:00', 'https://blog.example.com', 0.002, '198.51.100.2', '{"os":"macOS"}'),
    (2, 3, 6, '2024-02-02 09:00:00', 'https://shop.example.com', 0.003, '198.51.100.3', '{"os":"Windows"}'),
    (2, 4, 6, '2024-02-02 09:10:00', 'https://tech.example.com', 0.003, '198.51.100.4', '{"os":"iOS"}'),
    (2, 5, 7, '2024-02-03 07:00:00', 'https://news.example.com', 0.001, '198.51.100.5', '{"os":"Android"}'),
    (2, 6, 8, '2024-02-03 07:30:00', 'https://blog.example.com', 0.002, '198.51.100.6', '{"os":"Linux"}');

-- geo_ips reference data (same on every node)
insert into geo_ips (addrs, latitude, longitude)
values
    ('10.0.0.0/8',      37.77,  -122.41),
    ('172.16.0.0/12',   51.51,    -0.12),
    ('192.168.0.0/16',  35.69,   139.69),
    ('203.0.113.0/24',  40.71,   -74.01),
    ('198.51.100.0/24', 48.86,     2.35);
