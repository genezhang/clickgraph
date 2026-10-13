WITH with_w1 AS (
SELECT 
      v0.user_id AS `v1__user_id`, 
      v0.full_name AS `v2`, 
      v0.email_address AS `v3`
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v2 AS `n`, 
      w1.v3 AS `e`
FROM with_w1 AS w1
LIMIT 1
-- column n: Value [("n", "n")]
-- column e: Value [("e", "e")]