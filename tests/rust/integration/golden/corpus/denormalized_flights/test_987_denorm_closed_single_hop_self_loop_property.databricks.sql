SELECT 
      t0.OriginCityName AS `a.city`
FROM test_integration.flights AS t0
WHERE t0.Origin = t0.Dest
