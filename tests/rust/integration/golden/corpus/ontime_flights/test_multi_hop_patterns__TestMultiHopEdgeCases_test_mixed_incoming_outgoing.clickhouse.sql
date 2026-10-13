SELECT 
      v1.Origin AS "a.code", 
      v1.Dest AS "b.code", 
      v3.Origin AS "c.code"
FROM default.flights AS v1
JOIN default.flights AS v3 ON v3.Dest = v1.Dest
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v3.Origin IS NOT NULL AND (v1.flight_id <> v3.flight_id OR v1.flight_number <> v3.flight_number))
LIMIT 5
-- column a.code: Value [("a.code", "a.code")]
-- column b.code: Value [("b.code", "b.code")]
-- column c.code: Value [("c.code", "c.code")]