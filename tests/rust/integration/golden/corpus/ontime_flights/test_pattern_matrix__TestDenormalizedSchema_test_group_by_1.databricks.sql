SELECT `n.airport` AS `n.airport`, count(`n.code`) AS `cnt` FROM (
SELECT 
      n.airport AS `n.airport`,
      n.Origin AS `n.code`
FROM default.flights AS n
UNION DISTINCT 
SELECT 
      n.airport AS `n.airport`,
      n.Dest AS `n.code`
FROM default.flights AS n
) AS __union
GROUP BY `n.airport`
ORDER BY cnt DESC
LIMIT 10