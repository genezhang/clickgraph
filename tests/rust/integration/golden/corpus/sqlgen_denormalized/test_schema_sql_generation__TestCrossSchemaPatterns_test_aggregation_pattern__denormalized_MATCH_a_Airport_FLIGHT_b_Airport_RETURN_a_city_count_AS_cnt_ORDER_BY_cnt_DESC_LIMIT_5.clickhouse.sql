SELECT 
      t0.origin_city AS "a.city", 
      count(*) AS "cnt"
FROM db_denormalized.flights_denorm AS t0
GROUP BY t0.origin_city
ORDER BY cnt DESC
LIMIT 5