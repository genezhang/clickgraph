SELECT 
      v1.OriginCityName AS "origin.city", 
      v1.DestCityName AS "dest.city"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND (v1.OriginState = 'CA' AND v1.DestState = 'NY'))

-- column origin.city: Value [("origin.city", "origin.city")]
-- column dest.city: Value [("dest.city", "dest.city")]