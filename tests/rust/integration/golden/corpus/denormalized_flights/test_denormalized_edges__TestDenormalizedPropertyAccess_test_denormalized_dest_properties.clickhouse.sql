SELECT 
      f.DestCityName AS "dest.city", 
      f.DestState AS "dest.state"
FROM test_integration.flights AS f
WHERE f.flight_number = 'AA100'
