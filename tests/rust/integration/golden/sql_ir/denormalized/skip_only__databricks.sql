SELECT 
      t0.origin_code AS `a.code`
FROM db_denormalized.flights_denorm AS t0
ORDER BY t0.origin_code ASC
OFFSET 2