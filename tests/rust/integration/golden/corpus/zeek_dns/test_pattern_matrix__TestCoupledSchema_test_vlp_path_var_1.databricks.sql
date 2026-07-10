SELECT 
      1 AS `length(p)`, 
      array(t0.`id.orig_h`, t0.query) AS `nodes(p)`
FROM zeek.dns_log AS t0
LIMIT 5