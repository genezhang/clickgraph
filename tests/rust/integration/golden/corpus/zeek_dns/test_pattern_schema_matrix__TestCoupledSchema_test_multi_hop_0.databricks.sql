SELECT 
      r1.domain_name AS `a.domain_name`, 
      r1.domain_name AS `c.domain_name`
FROM zeek.dns_log AS r1
LIMIT 5