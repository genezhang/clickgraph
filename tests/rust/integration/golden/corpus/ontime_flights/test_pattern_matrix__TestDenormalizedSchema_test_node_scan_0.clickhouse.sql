SELECT `n.code` AS `n.code` FROM (
SELECT 
      n.Origin AS "n.code"
FROM default.flights AS n
UNION DISTINCT 
SELECT 
      n.Dest AS "n.code"
FROM default.flights AS n
) AS __union
LIMIT 10