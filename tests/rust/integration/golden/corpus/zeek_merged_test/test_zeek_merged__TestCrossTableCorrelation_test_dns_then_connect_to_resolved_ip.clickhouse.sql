SELECT DISTINCT 
      v1."id.orig_h" AS "src.ip", 
      v1.query AS "domain", 
      v3.answers AS "resolved", 
      v5."id.resp_h" AS "accessed"
FROM zeek.dns_log AS v1
JOIN zeek.dns_log AS v3 ON v3.query = v1.query
JOIN zeek.conn_log AS v5 ON v5."id.orig_h" = v1."id.orig_h"
WHERE (v1."id.orig_h" IS NOT NULL AND v1.query IS NOT NULL AND v3.answers IS NOT NULL AND v5."id.resp_h" IS NOT NULL)
ORDER BY v1.query ASC

-- column src.ip: Value [("src.ip", "src.ip")]
-- column domain: Value [("domain", "domain")]
-- column resolved: Value [("resolved", "resolved")]
-- column accessed: Value [("accessed", "accessed")]