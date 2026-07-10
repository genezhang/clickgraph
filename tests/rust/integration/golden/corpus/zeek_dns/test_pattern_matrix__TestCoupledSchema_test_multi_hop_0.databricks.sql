SELECT 
      r1.`id.orig_h` AS `a.ip_address`, 
      r2.ip_address AS `c.ip_address`
FROM zeek.dns_log AS r1
INNER JOIN zeek.dns_log AS r2 ON r2.`id.orig_h` = r1.query
WHERE r2.uid <> r1.uid
LIMIT 5