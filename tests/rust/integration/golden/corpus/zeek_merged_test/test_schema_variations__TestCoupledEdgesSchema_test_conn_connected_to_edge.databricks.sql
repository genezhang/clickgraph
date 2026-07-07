SELECT 
      t0.`id.resp_h` AS `dest.ip`
FROM zeek.conn_log AS t0
WHERE t0.`id.orig_h` = '192.168.1.10'
LIMIT 10