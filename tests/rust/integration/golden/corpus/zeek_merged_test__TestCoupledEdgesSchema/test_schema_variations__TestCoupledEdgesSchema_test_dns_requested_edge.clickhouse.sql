SELECT 
      v1.query AS "domain.name", 
      v1."id.orig_h" AS "ip.ip"
FROM zeek.dns_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1.query IS NOT NULL AND v1."id.orig_h" = '192.168.1.10')
LIMIT 10