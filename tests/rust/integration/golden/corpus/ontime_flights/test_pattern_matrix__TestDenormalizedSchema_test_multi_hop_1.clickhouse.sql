SELECT 
      v1.OriginState AS "a.state", 
      v3.DestState AS "c.state"
FROM default.flights AS v1
JOIN default.flights AS v3 ON v3.Origin = v1.Dest
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v3.Dest IS NOT NULL AND (v1.flight_id <> v3.flight_id OR v1.flight_number <> v3.flight_number))
LIMIT 5
-- column a.state: Value [("a.state", "a.state")]
-- column c.state: Value [("c.state", "c.state")]