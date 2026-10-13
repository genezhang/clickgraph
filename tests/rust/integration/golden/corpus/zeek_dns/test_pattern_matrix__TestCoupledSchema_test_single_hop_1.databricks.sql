SELECT 
      v1.query AS `a.domain_name`, 
      v1.answers AS `b.resolved_ip`
FROM zeek.dns_log AS v1
WHERE (v1.query IS NOT NULL AND v1.answers IS NOT NULL)
LIMIT 10
-- column a.domain_name: Value [("a.domain_name", "a.domain_name")]
-- column b.resolved_ip: Value [("b.resolved_ip", "b.resolved_ip")]