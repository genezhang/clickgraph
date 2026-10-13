SELECT DISTINCT 
      v1."id.orig_h" AS "srcip.ip", 
      v1.query AS "d.name", 
      v3."id.resp_h" AS "dest.ip"
FROM zeek.dns_log AS v1
JOIN zeek.conn_log AS v3 ON v3."id.orig_h" = v1."id.orig_h"
WHERE (v1."id.orig_h" IS NOT NULL AND v1.query IS NOT NULL AND v3."id.resp_h" IS NOT NULL AND v1."id.orig_h" = '192.168.1.10')

-- column srcip.ip: Value [("srcip.ip", "srcip.ip")]
-- column d.name: Value [("d.name", "d.name")]
-- column dest.ip: Value [("dest.ip", "dest.ip")]