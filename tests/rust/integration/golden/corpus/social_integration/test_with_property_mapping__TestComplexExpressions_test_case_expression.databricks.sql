WITH with_w1 AS (
SELECT 
      CASE WHEN v0.is_active THEN v0.full_name ELSE 'inactive' END AS `v1`
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS `status`
FROM with_w1 AS w1
LIMIT 1
-- column status: Value [("status", "status")]