SELECT 
      r.Origin AS "r.from_id", 
      r.Dest AS "r.to_id", 
      r.arr_time AS "r.arrival_time", 
      r.airline AS "r.carrier", 
      r.dep_time AS "r.departure_time", 
      r.distance_miles AS "r.distance", 
      r.flight_id AS "r.flight_id", 
      r.flight_number AS "r.flight_num"
FROM default.flights AS r
LIMIT 5