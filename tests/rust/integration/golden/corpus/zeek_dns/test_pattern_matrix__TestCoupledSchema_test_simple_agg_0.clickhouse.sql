SELECT count(`n.ip_address`) AS "count(n)" FROM (
SELECT 
      n."id.orig_h" AS "n.ip_address"
FROM zeek.dns_log AS n
UNION DISTINCT 
SELECT 
      n."id.resp_h" AS "n.ip_address"
FROM zeek.dns_log AS n
) AS __union
