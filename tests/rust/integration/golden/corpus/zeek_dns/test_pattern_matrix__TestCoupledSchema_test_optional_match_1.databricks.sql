WITH __denorm_scan_a AS (
SELECT 
      a.`id.orig_h` AS `ip_address`
FROM zeek.dns_log AS a
UNION DISTINCT 
SELECT 
      a.`id.resp_h` AS `ip_address`
FROM zeek.dns_log AS a

)
SELECT 
      a.ip_address AS `a.ip_address`, 
      count(r.uid) AS `rel_count`
FROM __denorm_scan_a AS a
LEFT JOIN zeek.dns_log AS r ON a.ip_address = r.`id.orig_h`
GROUP BY a.ip_address
