WITH with_w1 AS (
SELECT 
      __cg_element AS `v0`, 
      __cg_position AS `__o0`
FROM (SELECT 1) AS __cg_one
LATERAL VIEW posexplode(array(1, 2, 3)) AS __cg_position, __cg_element
), 
with_w2 AS (
SELECT 
      w1.v0 AS `v1`, 
      w1.__o0 AS `__o0`
FROM with_w1 AS w1
)
SELECT 
      count(w2.v1) AS `c`
FROM with_w2 AS w2

-- column c: Value [("c", "c")]