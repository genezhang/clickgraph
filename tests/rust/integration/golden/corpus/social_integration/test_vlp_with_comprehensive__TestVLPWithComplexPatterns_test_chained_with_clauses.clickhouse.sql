WITH with_w1 AS (
SELECT 
      1 AS "__row"
WHERE false
), 
with_w2 AS (
SELECT 
      1 AS "__row"
FROM with_w1 AS w1
)
SELECT 
      NULL AS "start_name", 
      NULL AS "end_name", 
      NULL AS "hops"
FROM with_w2 AS w2
LIMIT 5
-- column start_name: Value [("start_name", "start_name")]
-- column end_name: Value [("end_name", "end_name")]
-- column hops: Value [("hops", "hops")]