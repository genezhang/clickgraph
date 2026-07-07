SELECT `n.code` AS `n.code` FROM (
SELECT 
      n.Origin AS "n.code", 
      n.Origin AS "__order_col_0"
FROM default.flights AS n
WHERE n.Origin IS NOT NULL
UNION DISTINCT 
SELECT 
      n.Dest AS "n.code", 
      n.Dest AS "__order_col_0"
FROM default.flights AS n
WHERE n.Dest IS NOT NULL
) AS __union
ORDER BY __union.`__order_col_0` DESC
LIMIT 5, 10