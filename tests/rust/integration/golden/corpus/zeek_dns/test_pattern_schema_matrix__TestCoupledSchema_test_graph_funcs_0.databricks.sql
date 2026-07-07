SELECT 
      'REQUESTED' AS `type(r)`, 
      r.domain_name AS `id(a)`, 
      array('Domain') AS `labels(a)`
FROM zeek.dns_log AS r
LIMIT 5