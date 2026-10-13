SELECT 
      v1."id.orig_h" AS "src.ip", 
      v1."id.resp_h" AS "dst.ip", 
      v1.service AS "r.service", 
      v1.duration AS "r.duration"
FROM zeek.conn_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1."id.resp_h" IS NOT NULL AND v1.service = 'ssl')
ORDER BY v1.ts ASC
