SELECT 
      count(*) AS "flights"
FROM test_integration.flights AS f
WHERE (f.OriginCityName = 'Seattle' AND f.DestState = 'CA')
