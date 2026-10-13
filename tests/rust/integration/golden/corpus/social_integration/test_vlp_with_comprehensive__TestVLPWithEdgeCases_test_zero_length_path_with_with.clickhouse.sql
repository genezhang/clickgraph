WITH with_w1 AS (
SELECT 
      1 AS "__row"
WHERE false
)
SELECT 
      NULL AS "u.name", 
      NULL AS "hops"
FROM with_w1 AS w1

-- column u.name: Value [("u.name", "u.name")]
-- column hops: Value [("hops", "hops")]