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

-- column origin.code: Value [("origin.code", "origin.code")]
-- column origin.city: Value [("origin.city", "origin.city")]
-- column origin.state: Value [("origin.state", "origin.state")]
-- column dest.code: Value [("dest.code", "dest.code")]
-- column dest.city: Value [("dest.city", "dest.city")]
-- column dest.state: Value [("dest.state", "dest.state")]
-- column f.flight_num: Value [("f.flight_num", "f.flight_num")]
-- column f.carrier: Value [("f.carrier", "f.carrier")]
-- column f.distance: Value [("f.distance", "f.distance")]