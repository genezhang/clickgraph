SELECT 
      count(*) AS `count`
FROM test_integration.flights AS f
WHERE f.OriginCityName = 'Los Angeles'
