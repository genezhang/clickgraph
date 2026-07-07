SELECT 
      f.origin_city AS "origin.city", 
      f.dest_city AS "dest.city", 
      f.origin_state AS "state"
FROM db_denormalized.flights_denorm AS f
WHERE f.origin_state = f.dest_state
ORDER BY f.origin_city ASC
