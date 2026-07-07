SELECT 
      r.query AS `a.domain_name`, 
      r.answers AS `b.resolved_ip`
FROM zeek.dns_log AS r
LIMIT 10