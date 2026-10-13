SELECT 
      v1.Origin AS `a.code`, 
      v1.Dest AS `b.code`, 
      v3.Dest AS `c.code`, 
      v5.Dest AS `d.code`
FROM default.flights AS v1
JOIN default.flights AS v3 ON v3.Origin = v1.Dest
JOIN default.flights AS v5 ON v5.Origin = v3.Dest
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v3.Dest IS NOT NULL AND v5.Dest IS NOT NULL AND (v1.flight_id <> v3.flight_id OR v1.flight_number <> v3.flight_number) AND (v1.flight_id <> v5.flight_id OR v1.flight_number <> v5.flight_number) AND (v3.flight_id <> v5.flight_id OR v3.flight_number <> v5.flight_number))
LIMIT 5