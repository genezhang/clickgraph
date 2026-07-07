SELECT 
      t0.answers AS `a.resolved_ip`, 
      t0.answers AS `b.resolved_ip`
FROM zeek.dns_log AS t0
LIMIT 10