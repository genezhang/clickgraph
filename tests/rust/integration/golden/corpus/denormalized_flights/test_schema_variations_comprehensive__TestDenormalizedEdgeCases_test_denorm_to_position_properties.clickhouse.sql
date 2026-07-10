SELECT 
      f.DestState AS "dest.state", 
      count(*) AS "flights"
FROM test_integration.flights AS f
GROUP BY f.DestState
