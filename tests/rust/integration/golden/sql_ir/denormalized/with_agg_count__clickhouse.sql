WITH with_flights_origin_cte_0 AS (SELECT 
      t0.origin_code AS "origin", 
      count(t0.dest_code) AS "flights"
FROM db_denormalized.flights_denorm AS t0
GROUP BY t0.origin_code
)
SELECT 
      flights_origin.origin AS "origin", 
      flights_origin.flights AS "flights"
FROM with_flights_origin_cte_0 AS flights_origin
