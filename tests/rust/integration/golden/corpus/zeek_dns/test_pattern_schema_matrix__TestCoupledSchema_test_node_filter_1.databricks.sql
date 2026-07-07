SELECT `n.resolved_ip` AS `n.resolved_ip` FROM (
SELECT 
      n.answers AS `n.resolved_ip`
FROM zeek.dns_log AS n
WHERE n.answers IS NOT NULL
UNION DISTINCT 
SELECT 
      n.answers AS `n.resolved_ip`
FROM zeek.dns_log AS n
WHERE n.answers IS NOT NULL
) AS __union
LIMIT 10