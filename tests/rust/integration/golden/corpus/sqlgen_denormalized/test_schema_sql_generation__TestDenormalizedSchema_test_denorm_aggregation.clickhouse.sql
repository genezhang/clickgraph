SELECT 
      t0.origin_city AS "a.city", 
      count(*) AS "flights"
FROM db_denormalized.flights_denorm AS t0
GROUP BY t0.origin_city
ORDER BY flights DESC
LIMIT 10