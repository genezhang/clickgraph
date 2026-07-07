SELECT `n.city` AS `n.city` FROM (
SELECT 
      n.OriginCityName AS `n.city`
FROM default.flights AS n
UNION DISTINCT 
SELECT 
      n.DestCityName AS `n.city`
FROM default.flights AS n
) AS __union
LIMIT 10