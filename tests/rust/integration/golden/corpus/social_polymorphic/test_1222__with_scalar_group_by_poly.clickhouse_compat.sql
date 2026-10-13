WITH with_w1 AS (
SELECT 
      1 AS "__row"
FROM brahmand.users_bench AS v0
)
SELECT 
      NULL AS "ag", 
      count(*) AS "n"
FROM with_w1 AS w1
HAVING count(*) > 0

-- column ag: Value [("ag", "ag")]
-- column n: Value [("n", "n")]