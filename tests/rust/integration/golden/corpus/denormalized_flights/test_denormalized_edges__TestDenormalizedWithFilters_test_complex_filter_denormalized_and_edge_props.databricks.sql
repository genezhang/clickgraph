SELECT 
      f.OriginCityName AS `origin.city`, 
      f.DestCityName AS `dest.city`, 
      f.flight_number AS `f.flight_num`
FROM test_integration.flights AS f
WHERE ((f.OriginState = 'CA' AND f.DestState = 'CA') AND f.airline = 'American Airlines')
