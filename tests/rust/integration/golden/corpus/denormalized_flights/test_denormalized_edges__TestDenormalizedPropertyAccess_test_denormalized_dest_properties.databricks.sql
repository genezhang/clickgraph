SELECT 
      v1.DestCityName AS `dest.city`, 
      v1.DestState AS `dest.state`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v1.flight_number = 'AA100')

-- column dest.city: Value [("dest.city", "dest.city")]
-- column dest.state: Value [("dest.state", "dest.state")]