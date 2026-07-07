SELECT 
      f.dest_city AS `dest.city`, 
      f.dest_state AS `dest.state`
FROM db_denormalized.flights_denorm AS f
WHERE f.flight_number = 'AA100'
