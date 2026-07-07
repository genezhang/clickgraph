SELECT 
      f.origin_city AS "origin.city", 
      f.origin_state AS "origin.state"
FROM db_denormalized.flights_denorm AS f
WHERE f.flight_number = 'AA100'
