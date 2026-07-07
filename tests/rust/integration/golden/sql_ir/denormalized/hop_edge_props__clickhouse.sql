SELECT 
      r.origin_code AS "a.code", 
      r.carrier AS "r.carrier", 
      r.flight_number AS "r.flight_num", 
      r.distance AS "r.distance", 
      r.dest_code AS "b.code"
FROM db_denormalized.flights_denorm AS r
