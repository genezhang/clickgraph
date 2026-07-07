SELECT 
      r1.answers AS `a.resolved_ip`, 
      r1.answers AS `c.resolved_ip`
FROM zeek.dns_log AS r1
LIMIT 5