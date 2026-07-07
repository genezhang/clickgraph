SELECT 
      f.origin_city AS "origin.city", 
      count(*) AS "flights"
FROM db_denormalized.flights_denorm AS f
GROUP BY f.origin_city
