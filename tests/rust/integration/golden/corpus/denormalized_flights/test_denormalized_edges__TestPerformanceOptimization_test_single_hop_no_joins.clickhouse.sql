SELECT 
      f.origin_city AS "origin.city", 
      f.dest_city AS "dest.city", 
      f.carrier AS "f.carrier"
FROM db_denormalized.flights_denorm AS f
