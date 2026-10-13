WITH __cg_nodes_Domain__cg_rows AS (
SELECT 
      e.query AS `domain_name`
FROM zeek.dns_log AS e
), 
__cg_nodes_Domain AS (
SELECT 
      e.domain_name AS `domain_name`
FROM __cg_nodes_Domain__cg_rows AS e
WHERE NOT isnull(e.domain_name)
GROUP BY e.domain_name
)
SELECT 
      v0.domain_name AS `n.domain_name`
FROM __cg_nodes_Domain AS v0
LIMIT 5