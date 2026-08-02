WITH with_y_cte_0 AS (SELECT 
      x AS `y`
FROM (SELECT 1) AS _unwind
LATERAL VIEW explode(array(1, 2, 3)) AS x
)
SELECT 
      count(y.y) AS `c`
FROM with_y_cte_0 AS y
