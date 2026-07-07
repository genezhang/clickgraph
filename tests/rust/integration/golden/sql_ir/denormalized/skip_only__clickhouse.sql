SELECT 
      t0.origin_code AS "a.code"
FROM db_denormalized.flights_denorm AS t0
ORDER BY a.origin_code ASC
LIMIT 2, 18446744073709551615