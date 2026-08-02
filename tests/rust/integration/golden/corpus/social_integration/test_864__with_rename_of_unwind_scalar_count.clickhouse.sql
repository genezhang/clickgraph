WITH with_y_cte_0 AS (SELECT 
      x AS "y"
FROM system.one
ARRAY JOIN [1, 2, 3] AS x
)
SELECT 
      count(y.y) AS "c"
FROM with_y_cte_0 AS y
