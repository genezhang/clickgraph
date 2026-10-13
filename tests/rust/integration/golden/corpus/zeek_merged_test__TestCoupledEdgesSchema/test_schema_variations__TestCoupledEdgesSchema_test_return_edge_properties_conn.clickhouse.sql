SELECT 
      v1.service AS "r.service", 
      v1.duration AS "r.duration", 
      v1."id.resp_h" AS "dest.ip"
FROM zeek.conn_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1."id.resp_h" IS NOT NULL AND v1."id.orig_h" = '192.168.1.10')
LIMIT 5