SELECT 
      v1.OriginCityName AS "o.city", 
      v1.DestState AS "d.state", 
      count(*) AS "flight_count"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
GROUP BY v1.OriginCityName, v1.DestState

-- column o.city: Value [("o.city", "o.city")]
-- column d.state: Value [("d.state", "d.state")]
-- column flight_count: Value [("flight_count", "flight_count")]