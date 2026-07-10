SELECT 
      0 AS "length(p)", 
      nodes(p) AS "nodes(p)"
FROM zeek.dns_log AS t0
LIMIT 5