WITH with_w1 AS (
SELECT 
      1 AS `__row`
), 
with_w2 AS (
SELECT 
      1 AS `__row`
FROM with_w1 AS w1
)
SELECT 
      count(1) AS `c`
FROM with_w2 AS w2

-- column c: Value [("c", "c")]