WITH with_w1 AS (
SELECT 
      v0.full_name AS "v1", 
      v0.full_name AS "v2", 
      lowerUTF8(v0.full_name) AS "v3"
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS "n1", 
      w1.v2 AS "n2", 
      w1.v3 AS "lower_n"
FROM with_w1 AS w1
LIMIT 1
-- column n1: Value [("n1", "n1")]
-- column n2: Value [("n2", "n2")]
-- column lower_n: Value [("lower_n", "lower_n")]