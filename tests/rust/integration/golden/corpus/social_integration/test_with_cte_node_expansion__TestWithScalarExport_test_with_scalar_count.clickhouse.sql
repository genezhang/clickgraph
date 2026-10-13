WITH with_w1 AS (
SELECT 
      count(v0.user_id) AS "v1"
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS "user_count"
FROM with_w1 AS w1

-- column user_count: Value [("user_count", "user_count")]