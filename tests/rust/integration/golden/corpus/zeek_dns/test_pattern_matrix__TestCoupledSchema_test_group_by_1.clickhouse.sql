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
      v0.domain_name AS "n.domain_name", 
      count(v0.domain_name) AS "cnt"
FROM __cg_nodes_Domain AS v0
GROUP BY v0.domain_name
ORDER BY count(v0.domain_name) DESC NULLS FIRST
LIMIT 10
-- column n.domain_name: Value [("n.domain_name", "n.domain_name")]
-- column cnt: Value [("cnt", "cnt")]