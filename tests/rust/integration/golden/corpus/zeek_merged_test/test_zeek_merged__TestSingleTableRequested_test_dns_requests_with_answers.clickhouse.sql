SELECT 
      r."id.orig_h" AS "src.ip", 
      r.query AS "d.name", 
      r.answers AS "r.answers"
FROM zeek.dns_log AS r
WHERE r.query = 'cdn.example.com'
