SELECT 
      r."id.orig_h" AS "src.ip", 
      r.query AS "d.name", 
      r.qtype_name AS "r.qtype"
FROM zeek.dns_log AS r
WHERE r."id.orig_h" = '192.168.1.10'
ORDER BY r.ts ASC
