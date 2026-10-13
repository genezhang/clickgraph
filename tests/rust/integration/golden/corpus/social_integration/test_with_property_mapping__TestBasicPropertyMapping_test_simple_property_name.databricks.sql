WITH with_w1 AS (
SELECT 
      v0.full_name AS `v1`
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS `userName`
FROM with_w1 AS w1
LIMIT 1