WITH with_w1 AS (
SELECT 
      CASE WHEN count(*) >= 0 THEN [] ELSE [] END AS "v5", 
      CASE WHEN count(*) >= 0 THEN NULL ELSE NULL END AS "v6"
WHERE false
HAVING count(*) > 0
)
SELECT 
      NULL AS "u1.name", 
      w1.v5 AS "reachable", 
      w1.v6 AS "max_hops"
FROM with_w1 AS w1

-- column u1.name: Value [("u1.name", "u1.name")]
-- column reachable: Value [("reachable", "reachable")]
-- column max_hops: Value [("max_hops", "max_hops")]