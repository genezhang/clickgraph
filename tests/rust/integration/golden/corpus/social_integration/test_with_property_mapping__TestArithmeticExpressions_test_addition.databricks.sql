WITH with_w1 AS (
SELECT 
      v0.user_id + 100 AS `v1`
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS `offsetId`
FROM with_w1 AS w1
LIMIT 1
-- column offsetId: Value [("offsetId", "offsetId")]