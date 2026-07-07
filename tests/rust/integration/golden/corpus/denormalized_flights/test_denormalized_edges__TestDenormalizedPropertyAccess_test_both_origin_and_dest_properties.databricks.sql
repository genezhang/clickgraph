SELECT 
      f.origin_city AS `origin.city`, 
      f.dest_city AS `dest.city`, 
      f.flight_number AS `f.flight_num`
FROM db_denormalized.flights_denorm AS f
WHERE (f.origin_state = 'CA' AND f.dest_state = 'NY')
ORDER BY f.flight_number ASC
