WITH with_n_origin_cte_0 AS (SELECT 
      t0.origin_code AS "origin", 
      count(t0.dest_code) AS "n"
FROM db_denormalized.flights_denorm AS t0
GROUP BY t0.origin_code
HAVING n > 1
)
SELECT 
      n_origin.origin AS "origin", 
      n_origin.n AS "n"
FROM with_n_origin_cte_0 AS n_origin
