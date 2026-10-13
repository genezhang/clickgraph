SELECT 
      v1.OriginCityName AS "origin.city", 
      v1.DestState AS "dest.state"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
