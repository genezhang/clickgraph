SELECT 
      'RESOLVED_TO' AS `type(r)`, 
      r.answers AS `id(a)`, 
      array('ResolvedIP') AS `labels(a)`
FROM zeek.dns_log AS r
LIMIT 5