SELECT DISTINCT 
      t0.`id.orig_h` AS `src.ip`, 
      t1.query AS `domain`, 
      t1.answers AS `resolved`, 
      t0.`id.resp_h` AS `accessed`
FROM zeek.dns_log AS t1
JOIN zeek.conn_log AS t0 ON 1 = 1
ORDER BY domain ASC
