SELECT DISTINCT 
      t0.`id.orig_h` AS `src.ip`, 
      t1.query AS `domain`, 
      t1.answers AS `resolved`, 
      t0.`id.resp_h` AS `accessed`
FROM zeek.dns_log AS t1
INNER JOIN zeek.conn_log AS t0 ON t0.`id.orig_h` = t1.`id.orig_h`
ORDER BY domain ASC
