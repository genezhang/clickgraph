SELECT `n.domain_name` AS "n.domain_name", count(n.domain_name) AS "cnt" FROM (
SELECT 
      n.query AS "n.domain_name"
FROM zeek.dns_log AS n
UNION DISTINCT 
SELECT 
      n.query AS "n.domain_name"
FROM zeek.dns_log AS n
) AS __union
GROUP BY `n.domain_name`
ORDER BY cnt DESC
LIMIT 10