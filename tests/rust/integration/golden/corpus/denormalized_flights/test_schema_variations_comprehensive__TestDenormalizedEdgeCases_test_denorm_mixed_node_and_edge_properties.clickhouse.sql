SELECT 
      f.Origin AS "o.code", 
      f.OriginCityName AS "o.city", 
      f.flight_number AS "f.flight_number", 
      f.DestState AS "d.state"
FROM test_integration.flights AS f
