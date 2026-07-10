SELECT `b.code` AS `b.code`, `b.city` AS `b.city` FROM (
SELECT DISTINCT 
      t0.Dest AS "b.code", 
      t0.DestCityName AS "b.city"
FROM default.flights AS t0
WHERE t0.Origin = 'JFK'
UNION DISTINCT 
SELECT DISTINCT 
      t0.Origin AS "b.code", 
      t0.OriginCityName AS "b.city"
FROM default.flights AS t0
WHERE t0.Dest = 'JFK'
) AS __union
LIMIT 10