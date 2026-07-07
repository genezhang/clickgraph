SELECT 
      r.service AS "r.service", 
      r.duration AS "r.duration", 
      r."id.resp_h" AS "dest.ip"
FROM zeek.conn_log AS r
WHERE r."id.orig_h" = '192.168.1.10'
LIMIT 5