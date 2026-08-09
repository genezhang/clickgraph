WITH with_c_cte_0 AS (SELECT 
      t0.origin_code AS `c`
FROM db_denormalized.flights_denorm AS t0
ORDER BY c ASC NULLS LAST
LIMIT 3 OFFSET 1
)
SELECT 
      c.c AS `c`
FROM with_c_cte_0 AS c
