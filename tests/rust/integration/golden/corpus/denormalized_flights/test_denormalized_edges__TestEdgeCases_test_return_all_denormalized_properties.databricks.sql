SELECT 
      f.Origin AS `origin.code`, 
      f.OriginCityName AS `origin.city`, 
      f.OriginState AS `origin.state`, 
      f.Dest AS `dest.code`, 
      f.DestCityName AS `dest.city`, 
      f.DestState AS `dest.state`, 
      f.flight_number AS `f.flight_num`, 
      f.airline AS `f.carrier`, 
      f.distance_miles AS `f.distance`
FROM test_integration.flights AS f
WHERE f.flight_number = 'AA100'
