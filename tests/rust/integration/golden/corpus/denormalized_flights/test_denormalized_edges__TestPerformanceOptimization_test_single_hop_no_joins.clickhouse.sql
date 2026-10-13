SELECT 
      v1.OriginCityName AS "origin.city", 
      v1.DestCityName AS "dest.city", 
      v1.airline AS "f.carrier"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
