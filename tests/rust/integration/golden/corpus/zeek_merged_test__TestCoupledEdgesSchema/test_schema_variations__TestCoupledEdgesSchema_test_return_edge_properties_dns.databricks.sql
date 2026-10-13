SELECT 
      v1.qtype_name AS `r.qtype`, 
      v1.rcode_name AS `r.rcode`, 
      v1.query AS `domain.name`
FROM zeek.dns_log AS v1
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL AND v1.`id.orig_h` = '192.168.1.10')
LIMIT 5