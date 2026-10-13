WITH with_w1 AS (
SELECT 
      CASE WHEN count(*) >= 0 THEN array() ELSE array() END AS `v5`, 
      CASE WHEN count(*) >= 0 THEN NULL ELSE NULL END AS `v6`
WHERE false
HAVING count(*) > 0
)
SELECT 
      NULL AS `u1.name`, 
      w1.v5 AS `reachable`, 
      w1.v6 AS `max_hops`
FROM with_w1 AS w1
