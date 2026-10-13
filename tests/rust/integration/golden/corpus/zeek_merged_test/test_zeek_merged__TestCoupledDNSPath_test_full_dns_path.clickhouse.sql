SELECT 
      v1."id.orig_h" AS "src.ip", 
      v1.query AS "d.name", 
      v3.answers AS "rip.ip"
FROM zeek.dns_log AS v1
JOIN zeek.dns_log AS v3 ON v3.query = v1.query
WHERE (v1."id.orig_h" IS NOT NULL AND v1.query IS NOT NULL AND v3.answers IS NOT NULL AND v1."id.orig_h" = '192.168.1.10')
ORDER BY v1.query ASC

-- column src.ip: Value [("src.ip", "src.ip")]
-- column d.name: Value [("d.name", "d.name")]
-- column rip.ip: Value [("rip.ip", "rip.ip")]