SELECT `n.state` AS `n.state` FROM (
SELECT 
      n.OriginState AS `n.state`
FROM default.flights AS n
WHERE n.OriginState IS NOT NULL
UNION DISTINCT 
SELECT 
      n.DestState AS `n.state`
FROM default.flights AS n
WHERE n.DestState IS NOT NULL
) AS __union
LIMIT 10