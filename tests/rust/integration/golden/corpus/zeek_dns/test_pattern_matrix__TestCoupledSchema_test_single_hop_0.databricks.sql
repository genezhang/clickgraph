SELECT 
      v1.`id.orig_h` AS `a.ip_address`, 
      v1.query AS `b.domain_name`
FROM zeek.dns_log AS v1
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL)
LIMIT 10
-- column a.ip_address: Value [("a.ip_address", "a.ip_address")]
-- column b.domain_name: Value [("b.domain_name", "b.domain_name")]