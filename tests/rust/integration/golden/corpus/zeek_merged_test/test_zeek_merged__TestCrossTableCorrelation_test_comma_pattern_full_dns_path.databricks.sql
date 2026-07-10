SELECT 
      t0.`id.orig_h` AS `srcip.ip`, 
      t1.query AS `d.name`, 
      t1.answers AS `resolved_ip`, 
      t0.`id.resp_h` AS `accessed_ip`
FROM zeek.conn_log AS t0
INNER JOIN zeek.dns_log AS t1 ON t0.`id.orig_h` = t1.`id.orig_h`
WHERE t0.`id.orig_h` = '192.168.1.10'
ORDER BY t1.query ASC
