SELECT 
      v1.OriginCityName AS `a.city`, 
      v3.DestCityName AS `c.city`
FROM default.flights AS v1
JOIN default.flights AS v3 ON v3.Origin = v1.Dest
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v3.Dest IS NOT NULL AND (v1.flight_id <> v3.flight_id OR v1.flight_number <> v3.flight_number))
LIMIT 5