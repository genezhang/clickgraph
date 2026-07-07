SELECT count(`n.domain_name`) AS `count(n)` FROM (
SELECT 
      n.query AS `n.domain_name`
FROM zeek.dns_log AS n
UNION DISTINCT 
SELECT 
      n.query AS `n.domain_name`
FROM zeek.dns_log AS n
) AS __union
