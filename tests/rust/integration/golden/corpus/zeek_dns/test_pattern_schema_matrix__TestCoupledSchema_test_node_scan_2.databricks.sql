SELECT `n.resolved_ip` AS `n.resolved_ip` FROM (
SELECT 
      n.answers AS `n.resolved_ip`
FROM zeek.dns_log AS n
UNION DISTINCT 
SELECT 
      n.answers AS `n.resolved_ip`
FROM zeek.dns_log AS n
) AS __union
LIMIT 10