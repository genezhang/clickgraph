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
      v0.domain_name AS "n.domain_name"
FROM __cg_nodes_Domain AS v0
WHERE v0.domain_name IS NOT NULL
ORDER BY v0.domain_name DESC NULLS FIRST
LIMIT 5, 10