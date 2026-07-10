SELECT 
      f.OriginCityName AS `origin.city`, 
      f.DestCityName AS `dest.city`
FROM test_integration.flights AS f
WHERE (f.OriginState = 'CA' AND f.DestState = 'NY')
