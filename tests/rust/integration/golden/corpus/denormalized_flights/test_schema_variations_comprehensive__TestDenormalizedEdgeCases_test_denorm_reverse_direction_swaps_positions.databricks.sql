SELECT 
      f.origin_city AS `origin.city`, 
      f.dest_state AS `dest.state`
FROM db_denormalized.flights_denorm AS f
