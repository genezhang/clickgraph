SELECT count(*) AS "flights" FROM (
SELECT 1 AS __dummy
FROM test_integration.flights AS f
WHERE f.OriginCityName = 'Seattle'
UNION ALL 
SELECT 1 AS __dummy
FROM test_integration.flights AS f
WHERE f.DestCityName = 'Seattle'
) AS __union
