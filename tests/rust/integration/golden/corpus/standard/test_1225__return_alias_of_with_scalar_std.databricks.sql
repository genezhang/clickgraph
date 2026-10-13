WITH with_w1 AS (
SELECT 
      v0.age AS `v1`
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS `x`
FROM with_w1 AS w1
ORDER BY w1.v1 ASC NULLS LAST
