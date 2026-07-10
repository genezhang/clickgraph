SELECT 
      f.OriginCityName AS `origin.city`, 
      f.OriginState AS `origin.state`
FROM test_integration.flights AS f
WHERE f.flight_number = 'AA100'
