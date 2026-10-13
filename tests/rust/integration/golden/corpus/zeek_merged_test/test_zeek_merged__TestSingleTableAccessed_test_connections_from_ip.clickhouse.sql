SELECT 
      v1."id.orig_h" AS "src.ip", 
      v1."id.resp_h" AS "dst.ip", 
      v1.service AS "r.service"
FROM zeek.conn_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1."id.resp_h" IS NOT NULL AND v1."id.orig_h" = '192.168.1.10')
ORDER BY v1.ts ASC

-- column src.ip: Value [("src.ip", "src.ip")]
-- column dst.ip: Value [("dst.ip", "dst.ip")]
-- column r.service: Value [("r.service", "r.service")]