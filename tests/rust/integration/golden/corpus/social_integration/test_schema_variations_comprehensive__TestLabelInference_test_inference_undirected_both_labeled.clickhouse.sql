SELECT count(*) AS "total" FROM (
SELECT 1 AS __dummy
FROM test_integration.user_follows_test AS t0
UNION ALL 
SELECT 1 AS __dummy
FROM test_integration.user_follows_test AS t0
) AS __union
