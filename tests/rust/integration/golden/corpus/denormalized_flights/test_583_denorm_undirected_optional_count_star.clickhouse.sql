WITH __denorm_scan_a AS (
SELECT 
      "code" AS "code",
      min("city") AS "city",
      min("state") AS "state"
FROM (
SELECT 
      a.OriginCityName AS "city", 
      a.Origin AS "code", 
      a.OriginState AS "state"
FROM test_integration.flights AS a
UNION DISTINCT 
SELECT 
      a.DestCityName AS "city", 
      a.Dest AS "code", 
      a.DestState AS "state"
FROM test_integration.flights AS a

)
GROUP BY "code"

)
SELECT 
      count(*) AS "n"
FROM __denorm_scan_a AS a
LEFT JOIN (SELECT e.OriginCityName, e.DestCityName, e.Origin, e.Dest, e.OriginState, e.DestState, e.airline, e.arr_time, e.dep_time, e.distance_miles, e.flight_id, e.flight_number FROM test_integration.flights AS e UNION ALL SELECT e.DestCityName AS OriginCityName, e.OriginCityName AS DestCityName, e.Dest AS Origin, e.Origin AS Dest, e.DestState AS OriginState, e.OriginState AS DestState, e.airline, e.arr_time, e.dep_time, e.distance_miles, e.flight_id, e.flight_number FROM test_integration.flights AS e) AS t0 ON a.code = t0.Origin
