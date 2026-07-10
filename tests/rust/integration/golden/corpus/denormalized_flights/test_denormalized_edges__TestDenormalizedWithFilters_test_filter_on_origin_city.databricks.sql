SELECT 
      f.flight_number AS `f.flight_num`, 
      f.DestCityName AS `dest.city`
FROM test_integration.flights AS f
WHERE f.OriginCityName = 'Los Angeles'
ORDER BY f.flight_number ASC
