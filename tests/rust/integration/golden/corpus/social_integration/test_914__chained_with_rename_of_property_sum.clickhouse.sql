WITH with_w1 AS (
SELECT 
      v0.age AS "v1"
FROM test_integration.users_test AS v0
), 
with_w2 AS (
SELECT 
      w1.v1 AS "v2"
FROM with_w1 AS w1
)
SELECT 
      coalesce(sum(w2.v2), 0) AS "total"
FROM with_w2 AS w2

-- column total: Value [("total", "total")]