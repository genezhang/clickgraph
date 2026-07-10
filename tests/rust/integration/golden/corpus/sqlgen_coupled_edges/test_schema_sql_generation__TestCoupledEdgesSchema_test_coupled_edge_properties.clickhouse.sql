SELECT 
      r."id.orig_h" AS "ip.ip", 
      r.query AS "d.name", 
      r.ts AS "r.timestamp", 
      r.rcode_name AS "r.rcode"
FROM zeek.dns_log AS r
