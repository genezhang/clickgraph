WITH with_flights_origin_cte_0 AS (SELECT 
      t0.origin_city AS `origin`, 
      count(*) AS `flights`
FROM db_denormalized.flights_denorm AS t0
GROUP BY t0.origin_city
)
SELECT 
      flights_origin.origin AS `origin`, 
      flights_origin.flights AS `flights`
FROM with_flights_origin_cte_0 AS flights_origin
ORDER BY flights_origin.flights DESC
