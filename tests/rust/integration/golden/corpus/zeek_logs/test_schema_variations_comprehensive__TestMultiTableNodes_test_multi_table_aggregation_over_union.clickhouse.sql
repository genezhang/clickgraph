SELECT 
      target."id.orig_h" AS "ip.id.orig_h", 
      target."id.orig_h" AS "ip.ip", 
      count(r."id.orig_h") AS "connections"
FROM zeek.dns_log AS target
GROUP BY target."id.orig_h"
