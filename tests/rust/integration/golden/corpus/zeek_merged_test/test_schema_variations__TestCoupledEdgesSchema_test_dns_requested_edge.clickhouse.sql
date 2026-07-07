SELECT 
      t0.query AS "domain.name", 
      t0."id.orig_h" AS "ip.ip"
FROM zeek.dns_log AS t0
WHERE t0."id.orig_h" = '192.168.1.10'
LIMIT 10