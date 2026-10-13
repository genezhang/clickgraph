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
), 
optional_o3 AS (
SELECT 
      v1.query AS "v0__domain_name", 
      v1.uid AS "v1__uid", 
      v1.answers AS "v1__answers", 
      v1.query AS "v1__query", 
      v1.answers AS "v2__resolved_ip"
FROM zeek.dns_log AS v1
WHERE v1.answers IS NOT NULL
)
SELECT 
      v0.domain_name AS "a.domain_name", 
      count(o3.v1__uid) AS "rel_count"
FROM __cg_nodes_Domain AS v0
LEFT JOIN optional_o3 AS o3 ON v0.domain_name = o3.v0__domain_name
GROUP BY v0.domain_name
