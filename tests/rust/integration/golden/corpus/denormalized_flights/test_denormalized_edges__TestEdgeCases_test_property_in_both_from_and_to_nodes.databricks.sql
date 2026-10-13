SELECT 
      v1.OriginCityName AS `origin.city`, 
      v1.DestCityName AS `dest.city`, 
      v1.OriginState AS `state`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v1.OriginState = v1.DestState)
ORDER BY v1.OriginCityName ASC NULLS LAST

-- column origin.city: Value [("origin.city", "origin.city")]
-- column dest.city: Value [("dest.city", "dest.city")]
-- column state: Value [("state", "state")]