SELECT `n.domain_name` AS `n.domain_name` FROM (
SELECT 
      n.query AS `n.domain_name`
FROM zeek.dns_log AS n
WHERE n.query IS NOT NULL
UNION DISTINCT 
SELECT 
      n.query AS `n.domain_name`
FROM zeek.dns_log AS n
WHERE n.query IS NOT NULL
) AS __union
LIMIT 10