WITH __cg_nodes_Domain__cg_rows AS (
SELECT 
      e.query AS "domain_name"
FROM zeek.dns_log AS e
), 
__cg_nodes_Domain AS (
SELECT 
      e.domain_name AS "domain_name"
FROM __cg_nodes_Domain__cg_rows AS e
WHERE NOT isNull(e.domain_name)
GROUP BY e.domain_name
)
SELECT 
      count(v0.domain_name) AS "count(n)"
FROM __cg_nodes_Domain AS v0
