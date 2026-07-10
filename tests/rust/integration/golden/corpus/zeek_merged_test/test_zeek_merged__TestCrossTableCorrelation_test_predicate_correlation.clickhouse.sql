SELECT DISTINCT 
      t0."id.orig_h" AS "source", 
      t0.query AS "domain", 
      t1."id.resp_h" AS "accessed"
FROM zeek.dns_log AS t0
JOIN zeek.conn_log AS t1 ON 1 = 1
WHERE t0."id.orig_h" = t1."id.orig_h"
ORDER BY source ASC
