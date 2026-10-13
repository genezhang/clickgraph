SELECT 
      v1.origin_city AS "a.city", 
      count(*) AS "flights"
FROM db_denormalized.flights_denorm AS v1
WHERE (v1.origin_code IS NOT NULL AND v1.dest_code IS NOT NULL)
GROUP BY v1.origin_city
ORDER BY count(*) DESC NULLS FIRST
LIMIT 10