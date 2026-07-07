SELECT `n.state` AS `n.state` FROM (
SELECT 
      n.OriginState AS `n.state`
FROM default.flights AS n
UNION DISTINCT 
SELECT 
      n.DestState AS `n.state`
FROM default.flights AS n
) AS __union
LIMIT 10