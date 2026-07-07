SELECT 
      f.origin_code AS "o.code", 
      f.origin_city AS "o.city", 
      f.flight_number AS "f.flight_number", 
      f.dest_state AS "d.state"
FROM db_denormalized.flights_denorm AS f
