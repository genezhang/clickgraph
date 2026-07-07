SELECT `n.resolved_ip` AS `n.resolved_ip` FROM (
SELECT 
      n.answers AS `n.resolved_ip`, 
      n.answers AS `__order_col_0`
FROM zeek.dns_log AS n
WHERE n.answers IS NOT NULL
UNION DISTINCT 
SELECT 
      n.answers AS `n.resolved_ip`, 
      n.answers AS `__order_col_0`
FROM zeek.dns_log AS n
WHERE n.answers IS NOT NULL
) AS __union
ORDER BY __union.`__order_col_0` DESC
LIMIT 10 OFFSET 5