WITH with_y_cte_0 AS (SELECT 
      x AS "y"
FROM system.one
ARRAY JOIN [1, 2, 3] AS x
), 
with_z_cte_1 AS (SELECT 
      y.y AS "z"
FROM with_y_cte_0 AS y
WHERE y.y > 0
)
SELECT 
      count(z.z) AS "c"
FROM with_z_cte_1 AS z
