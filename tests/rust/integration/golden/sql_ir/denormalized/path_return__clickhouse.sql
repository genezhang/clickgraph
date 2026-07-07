SELECT 
      t0.origin_city AS "a.city", 
      t0.origin_code AS "a.code", 
      t0.origin_state AS "a.state", 
      t0.dest_city AS "b.city", 
      t0.dest_code AS "b.code", 
      t0.dest_state AS "b.state", 
      t0.arrival_time AS "t0.arrival_time", 
      t0.carrier AS "t0.carrier", 
      t0.departure_time AS "t0.departure_time", 
      t0.distance AS "t0.distance", 
      t0.flight_id AS "t0.flight_id", 
      t0.flight_number AS "t0.flight_num", 
      tuple('fixed_path', 'a', 'b', 't0') AS "p"
FROM db_denormalized.flights_denorm AS t0
