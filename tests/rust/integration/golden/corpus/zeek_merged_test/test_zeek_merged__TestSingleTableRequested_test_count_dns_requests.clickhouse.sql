SELECT 
      count(*) AS "total"
FROM zeek.dns_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1.query IS NOT NULL)

-- column total: Value [("total", "total")]