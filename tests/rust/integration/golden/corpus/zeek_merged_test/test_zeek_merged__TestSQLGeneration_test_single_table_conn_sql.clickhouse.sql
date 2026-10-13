SELECT 
      v1."id.orig_h" AS "src.ip", 
      v1."id.resp_h" AS "dst.ip", 
      v1.service AS "r.service"
FROM zeek.conn_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1."id.resp_h" IS NOT NULL AND v1."id.orig_h" = '192.168.1.10')
