SELECT 
      f.OriginCityName AS "origin.city", 
      f.DestCityName AS "dest.city", 
      f.OriginState AS "state"
FROM test_integration.flights AS f
WHERE f.OriginState = f.DestState
ORDER BY f.OriginCityName ASC
