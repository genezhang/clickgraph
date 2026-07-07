SELECT 
      'RESOLVED_TO' AS "type(r)", 
      r.query AS "id(a)", 
      ['Domain'] AS "labels(a)"
FROM zeek.dns_log AS r
LIMIT 5