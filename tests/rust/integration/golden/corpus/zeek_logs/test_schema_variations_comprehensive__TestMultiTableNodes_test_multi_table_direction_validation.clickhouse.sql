SELECT 
      count(*) AS "total"
FROM zeek.dns_log AS v0
JOIN zeek.dns_log AS v1 ON v1.query = v0.query
JOIN zeek.dns_log AS v2 ON v1."id.orig_h" = v2."id.orig_h"

-- column total: Value [("total", "total")]