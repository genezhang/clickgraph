SELECT 
      count(f.flight_id) AS "flight_count"
FROM test_integration.flights AS f
WHERE f.DestState = 'CA'
