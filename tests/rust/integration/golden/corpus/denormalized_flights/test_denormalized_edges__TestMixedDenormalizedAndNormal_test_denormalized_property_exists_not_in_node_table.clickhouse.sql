SELECT 
      count(*) AS "count"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v1.OriginCityName = 'Los Angeles')

-- column count: Value [("count", "count")]