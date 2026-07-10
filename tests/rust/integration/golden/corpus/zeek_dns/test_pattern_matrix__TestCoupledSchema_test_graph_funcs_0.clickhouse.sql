SELECT 
      'REQUESTED' AS "type(r)", 
      r."id.orig_h" AS "id(a)", 
      ['IP'] AS "labels(a)"
FROM zeek.dns_log AS r
LIMIT 5