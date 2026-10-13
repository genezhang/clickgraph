WITH with_w1 AS (
SELECT 
      substring(v0.full_name, (1) + 1, 5) AS `v1`
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS `prefix`
FROM with_w1 AS w1
LIMIT 1
-- column prefix: Value [("prefix", "prefix")]