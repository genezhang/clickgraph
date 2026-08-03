SELECT 
      count(*) AS `count(*)`
FROM test_integration.flights AS t0
WHERE t0.Origin = t0.Dest
