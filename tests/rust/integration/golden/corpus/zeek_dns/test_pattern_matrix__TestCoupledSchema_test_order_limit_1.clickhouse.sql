SELECT `n.domain_name` AS `n.domain_name` FROM (
SELECT 
      n.query AS "n.domain_name", 
      n.query AS "__order_col_0"
FROM zeek.dns_log AS n
WHERE n.query IS NOT NULL
UNION DISTINCT 
SELECT 
      n.query AS "n.domain_name", 
      n.query AS "__order_col_0"
FROM zeek.dns_log AS n
WHERE n.query IS NOT NULL
) AS __union
ORDER BY __union.`__order_col_0` DESC
LIMIT 5, 10