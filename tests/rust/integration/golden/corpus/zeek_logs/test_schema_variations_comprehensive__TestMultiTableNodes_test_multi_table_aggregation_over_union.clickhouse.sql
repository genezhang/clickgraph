SELECT 
      v0."id.orig_h" AS "ip.id.orig_h", 
      v0."id.orig_h" AS "ip.ip", 
      count(v1."id.orig_h") AS "connections"
FROM zeek.dns_log AS v0
JOIN zeek.dns_log AS v1 ON v1."id.orig_h" = v0."id.orig_h"
JOIN zeek.dns_log AS v2 ON v1.query = v2.query
GROUP BY v0."id.orig_h", v0."id.orig_h"
