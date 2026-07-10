SELECT `n.ip_address` AS `n.ip_address` FROM (
SELECT 
      n.`id.orig_h` AS `n.ip_address`, 
      n.`id.orig_h` AS `__order_col_0`
FROM zeek.dns_log AS n
WHERE n.`id.orig_h` IS NOT NULL
UNION DISTINCT 
SELECT 
      n.`id.resp_h` AS `n.ip_address`, 
      n.`id.resp_h` AS `__order_col_0`
FROM zeek.dns_log AS n
WHERE n.`id.resp_h` IS NOT NULL
) AS __union
ORDER BY __union.`__order_col_0` DESC
LIMIT 10 OFFSET 5