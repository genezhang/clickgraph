SELECT 
      f.origin_code AS "origin.code", 
      f.origin_city AS "origin.city", 
      f.origin_state AS "origin.state", 
      f.dest_code AS "dest.code", 
      f.dest_city AS "dest.city", 
      f.dest_state AS "dest.state", 
      f.flight_number AS "f.flight_num", 
      f.carrier AS "f.carrier", 
      f.distance AS "f.distance"
FROM db_denormalized.flights_denorm AS f
WHERE f.flight_number = 'AA100'
