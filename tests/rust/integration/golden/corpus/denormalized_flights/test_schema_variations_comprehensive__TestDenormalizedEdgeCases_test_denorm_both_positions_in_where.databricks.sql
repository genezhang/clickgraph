SELECT 
      count(*) AS `flights`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND (v1.OriginCityName = 'Seattle' AND v1.DestState = 'CA'))
