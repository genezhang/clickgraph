SELECT 
      v1.Origin AS "r.from_id", 
      v1.Dest AS "r.to_id", 
      v1.arr_time AS "r.arrival_time", 
      v1.airline AS "r.carrier", 
      v1.dep_time AS "r.departure_time", 
      v1.distance_miles AS "r.distance", 
      v1.flight_id AS "r.flight_id", 
      v1.flight_number AS "r.flight_num", 
      concat('FLIGHT:', concat(CASE WHEN v1.flight_id IS NULL THEN '\\N' ELSE replaceAll(replaceAll(replaceAll(toString(v1.flight_id), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END, '|', CASE WHEN v1.flight_number IS NULL THEN '\\N' ELSE replaceAll(replaceAll(replaceAll(toString(v1.flight_number), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END), ':', toString(v1.Origin), '->', toString(v1.Dest), '-') AS "r.elementId"
FROM default.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
LIMIT 5
-- column r: Rel { rel_type: "FLIGHT", from_label: "Airport", to_label: "Airport" } [("from_id", "r.from_id"), ("to_id", "r.to_id"), ("arrival_time", "r.arrival_time"), ("carrier", "r.carrier"), ("departure_time", "r.departure_time"), ("distance", "r.distance"), ("flight_id", "r.flight_id"), ("flight_num", "r.flight_num"), ("#elementId", "r.elementId")]