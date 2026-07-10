WITH __denorm_scan_a AS (
SELECT 
      a.query AS `domain_name`
FROM zeek.dns_log AS a
UNION DISTINCT 
SELECT 
      a.query AS `domain_name`
FROM zeek.dns_log AS a

)
SELECT 
      a.domain_name AS `a.domain_name`, 
      count(r.uid) AS `rel_count`
FROM __denorm_scan_a AS a
LEFT JOIN zeek.dns_log AS r ON a.domain_name = r.query
GROUP BY a.domain_name
