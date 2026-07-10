SELECT 
      r.qtype_name AS `r.qtype`, 
      r.rcode_name AS `r.rcode`, 
      r.query AS `domain.name`
FROM zeek.dns_log AS r
WHERE r.`id.orig_h` = '192.168.1.10'
LIMIT 5