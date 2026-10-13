SELECT 
      v1.OriginCityName AS `a.city`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest = v1.Origin)
