SELECT `n.resolved_ip` AS `n.resolved_ip`, `n.answers` AS `n.answers` FROM (
SELECT 
      n.answers AS "n.resolved_ip", 
      n.answers AS "n.answers"
FROM zeek.dns_log AS n
UNION DISTINCT 
SELECT 
      n.answers AS "n.resolved_ip", 
      n.answers AS "n.answers"
FROM zeek.dns_log AS n
) AS __union
LIMIT 5