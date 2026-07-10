SELECT `n.code` AS `n.code` FROM (
SELECT 
      n.Origin AS "n.code"
FROM default.flights AS n
WHERE n.Origin IS NOT NULL
UNION DISTINCT 
SELECT 
      n.Dest AS "n.code"
FROM default.flights AS n
WHERE n.Dest IS NOT NULL
) AS __union
LIMIT 10