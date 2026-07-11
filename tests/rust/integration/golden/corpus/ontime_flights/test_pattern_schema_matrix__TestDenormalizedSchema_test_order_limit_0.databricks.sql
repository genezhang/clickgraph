SELECT `n.state` AS `n.state` FROM (
SELECT 
      n.OriginState AS `n.state`, 
      n.OriginState AS `__order_col_0`
FROM default.flights AS n
WHERE n.OriginState IS NOT NULL
UNION DISTINCT 
SELECT 
      n.DestState AS `n.state`, 
      n.DestState AS `__order_col_0`
FROM default.flights AS n
WHERE n.DestState IS NOT NULL
) AS __union
ORDER BY __union.`__order_col_0` DESC
LIMIT 10 OFFSET 5