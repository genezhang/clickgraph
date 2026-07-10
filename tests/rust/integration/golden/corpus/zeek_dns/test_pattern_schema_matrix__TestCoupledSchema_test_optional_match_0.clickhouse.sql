WITH __denorm_scan_a AS (
SELECT 
      a."id.orig_h" AS "ip_address"
FROM zeek.dns_log AS a
UNION DISTINCT 
SELECT 
      a."id.resp_h" AS "ip_address"
FROM zeek.dns_log AS a

)
SELECT 
      r."id.orig_h" AS "a.ip_address", 
      count(*) AS "rel_count"
FROM __denorm_scan_a AS a
LEFT JOIN zeek.dns_log AS r ON 1 = 0
GROUP BY r."id.orig_h"
