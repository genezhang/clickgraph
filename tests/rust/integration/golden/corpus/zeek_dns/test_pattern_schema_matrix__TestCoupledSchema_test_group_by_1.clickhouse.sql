WITH __cg_nodes_ResolvedIP__cg_rows AS (
SELECT 
      e.answers AS "resolved_ip"
FROM zeek.dns_log AS e
), 
__cg_nodes_ResolvedIP AS (
SELECT 
      e.resolved_ip AS "resolved_ip"
FROM __cg_nodes_ResolvedIP__cg_rows AS e
WHERE NOT isNull(e.resolved_ip)
GROUP BY e.resolved_ip
)
SELECT 
      v0.resolved_ip AS "n.resolved_ip", 
      count(v0.resolved_ip) AS "cnt"
FROM __cg_nodes_ResolvedIP AS v0
GROUP BY v0.resolved_ip
ORDER BY count(v0.resolved_ip) DESC NULLS FIRST
LIMIT 10
-- column n.resolved_ip: Value [("n.resolved_ip", "n.resolved_ip")]
-- column cnt: Value [("cnt", "cnt")]