SELECT count(`n.code`) AS `count(n)` FROM (
SELECT 
      n.Origin AS `n.code`
FROM default.flights AS n
UNION DISTINCT 
SELECT 
      n.Dest AS `n.code`
FROM default.flights AS n
) AS __union
