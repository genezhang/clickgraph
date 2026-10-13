WITH with_w1 AS (
SELECT 
      v0.full_name AS "v1", 
      v0.full_name AS "__o0"
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS "userName"
FROM with_w1 AS w1
ORDER BY w1.__o0 ASC
LIMIT 5
-- column userName: Value [("userName", "userName")]