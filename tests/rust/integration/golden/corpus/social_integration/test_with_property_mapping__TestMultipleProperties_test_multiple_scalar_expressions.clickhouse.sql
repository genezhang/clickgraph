WITH with_w1 AS (
SELECT 
      v0.full_name AS "v1", 
      v0.email_address AS "v2", 
      v0.user_id AS "v3"
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS "n", 
      w1.v2 AS "e", 
      w1.v3 AS "id"
FROM with_w1 AS w1
LIMIT 1