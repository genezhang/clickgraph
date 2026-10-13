SELECT 
      v1.flight_number AS "f.flight_num", 
      v1.DestCityName AS "dest.city"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v1.OriginCityName = 'Los Angeles')
ORDER BY v1.flight_number ASC

-- column f.flight_num: Value [("f.flight_num", "f.flight_num")]
-- column dest.city: Value [("dest.city", "dest.city")]