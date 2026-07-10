SELECT 
      r.answers AS "a.resolved_ip", 
      r.query AS "b.domain_name"
FROM zeek.dns_log AS r
LIMIT 10