SELECT 
      f.OriginCityName AS `origin.city`, 
      count(*) AS `flights`
FROM test_integration.flights AS f
GROUP BY f.OriginCityName
