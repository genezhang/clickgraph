SELECT 
      t0.`id.orig_h` AS `a.ip`, 
      t0.`id.resp_h` AS `b.ip`
FROM zeek.conn_log AS t0
UNION ALL 
SELECT 
      t0.`id.resp_h` AS `a.ip`, 
      t0.`id.orig_h` AS `b.ip`
FROM zeek.conn_log AS t0
