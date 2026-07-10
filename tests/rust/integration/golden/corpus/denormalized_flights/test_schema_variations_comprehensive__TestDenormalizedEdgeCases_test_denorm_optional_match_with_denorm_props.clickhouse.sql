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
      a.code AS "a.code", 
      f.DestCityName AS "b.city", 
      count(f.flight_id) AS "flights"
FROM __denorm_scan_a AS a
LEFT JOIN test_integration.flights AS f ON a.code = f.Origin
GROUP BY a.code, f.DestCityName
