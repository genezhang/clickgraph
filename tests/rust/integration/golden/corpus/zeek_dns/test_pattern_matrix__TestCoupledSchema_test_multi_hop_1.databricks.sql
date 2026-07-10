SELECT 
      r1.domain_name AS `a.domain_name`, 
      r2.query AS `c.domain_name`
FROM zeek.dns_log AS r1
INNER JOIN zeek.dns_log AS r2 ON r2.`id.orig_h` = r1.query
WHERE r2.uid <> r1.uid
LIMIT 5