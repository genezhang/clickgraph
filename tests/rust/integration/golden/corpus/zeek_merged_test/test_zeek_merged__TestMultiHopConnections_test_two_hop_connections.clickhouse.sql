SELECT 
      r1."id.orig_h" AS "a.ip", 
      r2."id.orig_h" AS "b.ip", 
      r2."id.resp_h" AS "c.ip", 
      r1.service AS "svc1", 
      r2.service AS "svc2"
FROM zeek.conn_log AS r1
INNER JOIN zeek.conn_log AS r2 ON r2."id.orig_h" = r1."id.resp_h"
ORDER BY r1."id.orig_h" ASC
