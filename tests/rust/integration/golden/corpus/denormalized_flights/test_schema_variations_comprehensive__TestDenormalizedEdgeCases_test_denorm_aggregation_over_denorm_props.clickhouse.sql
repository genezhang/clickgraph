SELECT 
      f.OriginCityName AS "o.city", 
      f.DestState AS "d.state", 
      count(*) AS "flight_count"
FROM test_integration.flights AS f
GROUP BY f.OriginCityName, f.DestState
