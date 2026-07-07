SELECT 
      t0.origin_code AS "a.code", 
      t0.dest_code AS "b.code"
FROM db_denormalized.flights_denorm AS t0
ORDER BY a.origin_code DESC
LIMIT 1, 3