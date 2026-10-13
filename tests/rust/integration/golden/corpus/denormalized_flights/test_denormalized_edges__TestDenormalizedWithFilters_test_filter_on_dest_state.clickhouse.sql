SELECT 
      count(v1.flight_id) AS "flight_count"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v1.DestState = 'CA')
