WITH with_w1 AS (
SELECT 
      v1."id.orig_h" AS "v3", 
      v1.query AS "v4"
FROM zeek.dns_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1.query IS NOT NULL)
)
SELECT DISTINCT 
      w1.v3 AS "source_ip", 
      w1.v4 AS "domain", 
      v6."id.resp_h" AS "dest_ip"
FROM with_w1 AS w1
CROSS JOIN zeek.conn_log AS v6
WHERE (v6."id.orig_h" IS NOT NULL AND v6."id.resp_h" IS NOT NULL AND v6."id.orig_h" = w1.v3)
ORDER BY w1.v3 ASC, w1.v4 ASC
