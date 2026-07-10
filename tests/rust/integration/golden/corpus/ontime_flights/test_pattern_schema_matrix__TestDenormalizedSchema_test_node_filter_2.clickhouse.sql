SELECT `n.airport` AS `n.airport` FROM (
SELECT 
      n.airport AS "n.airport"
FROM default.flights AS n
WHERE n.airport IS NOT NULL
UNION DISTINCT 
SELECT 
      n.airport AS "n.airport"
FROM default.flights AS n
WHERE n.airport IS NOT NULL
) AS __union
LIMIT 10