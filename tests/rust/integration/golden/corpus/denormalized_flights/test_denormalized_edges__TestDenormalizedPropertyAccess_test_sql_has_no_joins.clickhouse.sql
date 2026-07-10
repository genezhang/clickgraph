SELECT 
      f.OriginCityName AS "origin.city", 
      f.DestCityName AS "dest.city", 
      f.airline AS "f.carrier"
FROM test_integration.flights AS f
LIMIT 1