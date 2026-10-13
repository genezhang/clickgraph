WITH with_w1 AS (
SELECT 
      1 AS "__row"
WHERE false
)
SELECT 
      NULL AS "u1.name", 
      NULL AS "u2.name", 
      NULL AS "hops", 
      NULL * 2 AS "doubled_hops"
FROM with_w1 AS w1

-- column u1.name: Value [("u1.name", "u1.name")]
-- column u2.name: Value [("u2.name", "u2.name")]
-- column hops: Value [("hops", "hops")]
-- column doubled_hops: Value [("doubled_hops", "doubled_hops")]