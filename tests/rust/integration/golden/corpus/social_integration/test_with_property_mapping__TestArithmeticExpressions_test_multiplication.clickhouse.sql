WITH with_w1 AS (
SELECT 
      v0.user_id * 2 AS "v1"
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS "doubled"
FROM with_w1 AS w1
LIMIT 1