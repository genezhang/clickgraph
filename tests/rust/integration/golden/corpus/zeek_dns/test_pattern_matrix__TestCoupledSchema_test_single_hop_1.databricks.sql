SELECT 
      v1.query AS `a.domain_name`, 
      v1.answers AS `b.resolved_ip`
FROM zeek.dns_log AS v1
WHERE (v1.query IS NOT NULL AND v1.answers IS NOT NULL)
LIMIT 10