SELECT 
      t0.`id.orig_h` AS `a.ip`, 
      t1.`id.orig_h` AS `b.ip`, 
      t1.`id.resp_h` AS `c.ip`
FROM zeek.conn_log AS t0
INNER JOIN zeek.conn_log AS t1 ON t1.`id.orig_h` = t0.`id.resp_h`
