SELECT 
      v1.OriginCityName AS `origin.city`, 
      v1.DestCityName AS `dest.city`, 
      v1.flight_number AS `f.flight_num`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND ((v1.OriginState = 'CA' AND v1.DestState = 'CA') AND v1.airline = 'American Airlines'))

-- column origin.city: Value [("origin.city", "origin.city")]
-- column dest.city: Value [("dest.city", "dest.city")]
-- column f.flight_num: Value [("f.flight_num", "f.flight_num")]