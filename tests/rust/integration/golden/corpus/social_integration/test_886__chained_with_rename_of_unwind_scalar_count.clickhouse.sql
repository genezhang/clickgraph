WITH with_w1 AS (
SELECT 
      __cg_element AS "v0", 
      __cg_position AS "__o0"
FROM system.one AS __cg_one
ARRAY JOIN [1, 2, 3] AS __cg_element, arrayEnumerate([1, 2, 3]) AS __cg_position
), 
with_w2 AS (
SELECT 
      w1.v0 AS "v1", 
      w1.__o0 AS "__o0"
FROM with_w1 AS w1
), 
with_w3 AS (
SELECT 
      w2.v1 AS "v2", 
      w2.__o0 AS "__o0"
FROM with_w2 AS w2
)
SELECT 
      count(w3.v2) AS "c"
FROM with_w3 AS w3
