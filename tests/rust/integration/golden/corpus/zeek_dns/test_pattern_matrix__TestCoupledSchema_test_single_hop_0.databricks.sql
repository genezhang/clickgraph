SELECT 
      r.`id.orig_h` AS `a.ip_address`, 
      r.query AS `b.domain_name`
FROM zeek.dns_log AS r
LIMIT 10