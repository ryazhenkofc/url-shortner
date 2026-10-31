-- 100k links; the first 7 hex chars of md5 are valid base62 codes
INSERT INTO links (code, long_url, owner_id)
SELECT substr(md5(i::text), 1, 7), 'https://example.com/page/' || i, 1
FROM generate_series(1, 100000) AS i
ON CONFLICT DO NOTHING;
