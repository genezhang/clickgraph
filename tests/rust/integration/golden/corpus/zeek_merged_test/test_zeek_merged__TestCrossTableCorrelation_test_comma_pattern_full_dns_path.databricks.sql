SELECT 
      v1.`id.orig_h` AS `srcip.ip`, 
      v1.query AS `d.name`, 
      v3.answers AS `resolved_ip`, 
      v5.`id.resp_h` AS `accessed_ip`
FROM zeek.dns_log AS v1
JOIN zeek.dns_log AS v3 ON v3.query = v1.query
JOIN zeek.conn_log AS v5 ON v5.`id.orig_h` = v1.`id.orig_h`
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL AND v3.answers IS NOT NULL AND v5.`id.resp_h` IS NOT NULL AND v1.`id.orig_h` = '192.168.1.10')
ORDER BY v1.query ASC NULLS LAST
