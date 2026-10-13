SELECT 
      v1.Origin AS "o.code", 
      v1.OriginCityName AS "o.city", 
      v1.flight_number AS "f.flight_number", 
      v1.DestState AS "d.state"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)

-- column o.code: Value [("o.code", "o.code")]
-- column o.city: Value [("o.city", "o.city")]
-- column f.flight_number: Value [("f.flight_number", "f.flight_number")]
-- column d.state: Value [("d.state", "d.state")]