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
WHERE a.Origin = 'JFK'
UNION DISTINCT 
SELECT 
      a.DestCityName AS "city", 
      a.Dest AS "code", 
      a.DestState AS "state"
FROM test_integration.flights AS a
WHERE a.Dest = 'JFK'

)
GROUP BY "code"

)
SELECT 
      a.code AS "a.code", 
      f.Dest AS "b.code", 
      g.Dest AS "c.code"
FROM __denorm_scan_a AS a
LEFT JOIN test_integration.flights AS f ON a.code = f.Origin
LEFT JOIN test_integration.flights AS g ON g.Origin = f.Dest
ORDER BY a.code ASC, f.Dest ASC, g.Dest ASC
