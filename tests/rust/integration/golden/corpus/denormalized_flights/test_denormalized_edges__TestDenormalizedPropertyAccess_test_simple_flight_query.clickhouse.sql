SELECT 
      v1.flight_number AS "f.flight_num", 
      v1.airline AS "f.carrier"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)

-- column f.flight_num: Value [("f.flight_num", "f.flight_num")]
-- column f.carrier: Value [("f.carrier", "f.carrier")]