SELECT 
      f.origin_city AS `origin.city`, 
      f.dest_city AS `dest.city`
FROM db_denormalized.flights_denorm AS f
WHERE (f.origin_state = 'CA' AND f.dest_state = 'NY')
