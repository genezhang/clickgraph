WITH with_w1 AS (
SELECT 
      v0.age AS "v1"
FROM brahmand.users_bench AS v0
)
SELECT 
      w1.v1 AS "ag", 
      count(*) AS "n"
FROM with_w1 AS w1
GROUP BY w1.v1

-- column ag: Value [("ag", "ag")]
-- column n: Value [("n", "n")]