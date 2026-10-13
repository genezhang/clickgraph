SELECT 
      v1."id.orig_h" AS "ip.ip", 
      v1.query AS "d.name"
FROM zeek.dns_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1.query IS NOT NULL)
