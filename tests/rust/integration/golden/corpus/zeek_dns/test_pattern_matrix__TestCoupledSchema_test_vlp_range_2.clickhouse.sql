SELECT 
      t0."id.orig_h" AS "a.ip_address", 
      t0.ip_address AS "b.ip_address"
FROM zeek.dns_log AS t0
LIMIT 10