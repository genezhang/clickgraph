SELECT DISTINCT 
      v1.`id.orig_h` AS `source`, 
      v1.query AS `domain`, 
      v4.`id.resp_h` AS `accessed`
FROM zeek.dns_log AS v1
CROSS JOIN zeek.conn_log AS v4
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL AND v4.`id.orig_h` IS NOT NULL AND v4.`id.resp_h` IS NOT NULL AND v1.`id.orig_h` = v4.`id.orig_h`)
ORDER BY `source` ASC NULLS LAST
