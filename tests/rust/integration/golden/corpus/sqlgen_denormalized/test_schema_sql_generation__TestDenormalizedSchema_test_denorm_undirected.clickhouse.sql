SELECT `b.city` AS "b.city", count(*) AS "flights" FROM (
SELECT 
      t0.dest_city AS "b.city"
FROM db_denormalized.flights_denorm AS t0
WHERE t0.origin_city = 'Seattle'
UNION ALL 
SELECT 
      t0.origin_city AS "b.city"
FROM db_denormalized.flights_denorm AS t0
WHERE t0.dest_city = 'Seattle'
) AS __union
GROUP BY `b.city`
