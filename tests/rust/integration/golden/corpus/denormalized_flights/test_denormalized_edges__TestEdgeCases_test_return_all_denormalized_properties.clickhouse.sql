SELECT 
      v1.Origin AS "origin.code", 
      v1.OriginCityName AS "origin.city", 
      v1.OriginState AS "origin.state", 
      v1.Dest AS "dest.code", 
      v1.DestCityName AS "dest.city", 
      v1.DestState AS "dest.state", 
      v1.flight_number AS "f.flight_num", 
      v1.airline AS "f.carrier", 
      v1.distance_miles AS "f.distance"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v1.flight_number = 'AA100')
