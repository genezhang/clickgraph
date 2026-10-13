SELECT 
      v1.`id.orig_h` AS `ip.ip`, 
      v1.query AS `d.name`, 
      v3.answers AS `rip.ip`
FROM zeek.dns_log AS v1
JOIN zeek.dns_log AS v3 ON v3.query = v1.query
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL AND v3.answers IS NOT NULL)
