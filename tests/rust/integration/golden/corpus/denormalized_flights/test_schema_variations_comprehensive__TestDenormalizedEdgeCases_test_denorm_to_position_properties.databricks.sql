SELECT 
      v1.DestState AS `dest.state`, 
      count(*) AS `flights`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
GROUP BY v1.DestState
