SELECT 
      r.origin_code AS "a.code", 
      r.dest_code AS "b.code"
FROM db_denormalized.flights_denorm AS r
WHERE r.distance > 1000
