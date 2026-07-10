SELECT `n.city` AS `n.city` FROM (
SELECT 
      n.OriginCityName AS "n.city"
FROM default.flights AS n
WHERE n.OriginCityName IS NOT NULL
UNION DISTINCT 
SELECT 
      n.DestCityName AS "n.city"
FROM default.flights AS n
WHERE n.DestCityName IS NOT NULL
) AS __union
LIMIT 10