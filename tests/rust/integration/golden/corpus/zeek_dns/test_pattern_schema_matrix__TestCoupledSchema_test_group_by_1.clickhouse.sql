SELECT `n.resolved_ip` AS "n.resolved_ip", count(`n.answers`) AS "cnt" FROM (
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
GROUP BY `n.resolved_ip`
ORDER BY `cnt` DESC
LIMIT 10