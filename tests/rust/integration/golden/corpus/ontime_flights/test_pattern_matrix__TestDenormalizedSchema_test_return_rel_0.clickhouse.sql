SELECT 
      v1.Origin AS "r.from_id", 
      v1.Dest AS "r.to_id", 
      v1.arr_time AS "r.arrival_time", 
      v1.airline AS "r.carrier", 
      v1.dep_time AS "r.departure_time", 
      v1.distance_miles AS "r.distance", 
      v1.flight_id AS "r.flight_id", 
      v1.flight_number AS "r.flight_num"
FROM default.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
LIMIT 5