SELECT 
      t0.`id.orig_h` AS `ip.ip`, 
      t0.`id.resp_h` AS `other.ip`
FROM zeek.conn_log AS t0
UNION ALL 
SELECT 
      t0.`id.resp_h` AS `ip.ip`, 
      t0.`id.orig_h` AS `other.ip`
FROM zeek.conn_log AS t0
