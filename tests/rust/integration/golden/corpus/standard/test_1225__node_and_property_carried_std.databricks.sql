WITH with_w1 AS (
SELECT 
      v0.user_id AS `v1__user_id`, 
      v0.full_name AS `v2`
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v2 AS `n`
FROM with_w1 AS w1
