WITH with_w1 AS (
SELECT 
      v0.age AS "v1"
FROM test_integration.users_test AS v0
), 
with_w2 AS (
SELECT 
      w1.v1 AS "v2", 
      count(*) AS "v3"
FROM with_w1 AS w1
GROUP BY w1.v1
)
SELECT 
      w2.v2 AS "ag", 
      w2.v3 AS "c"
FROM with_w2 AS w2
ORDER BY w2.v2 ASC

-- column ag: Value [("ag", "ag")]
-- column c: Value [("c", "c")]