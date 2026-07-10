SELECT 
      r.query AS "a.domain_name", 
      r.domain_name AS "b.domain_name"
FROM zeek.dns_log AS r
LIMIT 10