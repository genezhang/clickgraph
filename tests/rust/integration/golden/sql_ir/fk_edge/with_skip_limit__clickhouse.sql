WITH with_a_cte_0 AS (SELECT 
      o.total_amount AS "a"
FROM db_fk_edge.orders_fk AS o
ORDER BY a ASC
LIMIT 1, 2
)
SELECT 
      a.a AS "a"
FROM with_a_cte_0 AS a
