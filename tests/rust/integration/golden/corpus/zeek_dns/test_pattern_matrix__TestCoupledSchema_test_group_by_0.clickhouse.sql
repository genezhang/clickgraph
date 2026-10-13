WITH __cg_nodes_IP__cg_rows AS (
SELECT 
      e."id.orig_h" AS "ip_address"
FROM zeek.dns_log AS e

UNION ALL
SELECT 
      e."id.resp_h" AS "ip_address"
FROM zeek.dns_log AS e
), 
__cg_nodes_IP AS (
SELECT 
      e.ip_address AS "ip_address"
FROM __cg_nodes_IP__cg_rows AS e
WHERE NOT isNull(e.ip_address)
GROUP BY e.ip_address
)
SELECT 
      v0.ip_address AS "n.ip_address", 
      count(v0.ip_address) AS "cnt"
FROM __cg_nodes_IP AS v0
GROUP BY v0.ip_address
ORDER BY count(v0.ip_address) DESC NULLS FIRST
LIMIT 10
-- column n.ip_address: Value [("n.ip_address", "n.ip_address")]
-- column cnt: Value [("cnt", "cnt")]