SELECT 
      v1.OriginCityName AS `origin.city`, 
      v1.OriginState AS `origin.state`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v1.flight_number = 'AA100')

-- column origin.city: Value [("origin.city", "origin.city")]
-- column origin.state: Value [("origin.state", "origin.state")]