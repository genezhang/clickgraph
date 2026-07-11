SELECT `n.city` AS `n.city` FROM (
SELECT 
      n.OriginCityName AS `n.city`, 
      n.OriginCityName AS `__order_col_0`
FROM default.flights AS n
WHERE n.OriginCityName IS NOT NULL
UNION DISTINCT 
SELECT 
      n.DestCityName AS `n.city`, 
      n.DestCityName AS `__order_col_0`
FROM default.flights AS n
WHERE n.DestCityName IS NOT NULL
) AS __union
ORDER BY __union.`__order_col_0` DESC
LIMIT 10 OFFSET 5