SELECT 
      r.`id.orig_h` AS `src.ip`, 
      r.`id.resp_h` AS `dst.ip`, 
      r.service AS `r.service`
FROM zeek.conn_log AS r
WHERE r.`id.orig_h` = '192.168.1.10'
ORDER BY r.ts ASC
