SELECT 
      r.`id.orig_h` AS `src.ip`, 
      r.`id.resp_h` AS `dst.ip`, 
      r.service AS `r.service`, 
      r.duration AS `r.duration`
FROM zeek.conn_log AS r
WHERE r.service = 'ssl'
ORDER BY r.ts ASC
