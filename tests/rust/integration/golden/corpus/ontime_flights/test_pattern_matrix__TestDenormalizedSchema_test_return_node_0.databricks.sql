SELECT `n.city` AS `n.city`, `n.code` AS `n.code`, `n.state` AS `n.state` FROM (
SELECT 
      n.OriginCityName AS `n.city`, 
      n.Origin AS `n.code`, 
      n.OriginState AS `n.state`
FROM default.flights AS n
UNION DISTINCT 
SELECT 
      n.DestCityName AS `n.city`, 
      n.Dest AS `n.code`, 
      n.DestState AS `n.state`
FROM default.flights AS n
) AS __union
LIMIT 5