SELECT `n.state` AS `n.state`, count(`n.code`) AS `cnt` FROM (
SELECT 
      n.OriginState AS `n.state`,
      n.Origin AS `n.code`
FROM default.flights AS n
UNION DISTINCT 
SELECT 
      n.DestState AS `n.state`,
      n.Dest AS `n.code`
FROM default.flights AS n
) AS __union
GROUP BY `n.state`
ORDER BY cnt DESC
LIMIT 10