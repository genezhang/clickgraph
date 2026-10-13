WITH with_w1 AS (
SELECT 
      1 AS `__row`
WHERE false
LIMIT 10), 
with_w2 AS (
SELECT 
      1 AS `__row`
FROM with_w1 AS w1
WHERE NULL >= 1
)
SELECT 
      NULL AS `u1.name`, 
      NULL AS `u2.name`, 
      NULL AS `hops`
FROM with_w2 AS w2
