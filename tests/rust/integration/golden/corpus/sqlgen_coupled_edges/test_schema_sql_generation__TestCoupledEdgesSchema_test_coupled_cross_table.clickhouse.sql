WITH __cg_nodes_IP__cg_rows AS (
SELECT 
      e."id.orig_h" AS "ip"
FROM zeek.dns_log AS e

UNION ALL
SELECT 
      e."id.resp_h" AS "ip"
FROM zeek.dns_log AS e
), 
__cg_nodes_IP AS (
SELECT 
      e.ip AS "ip"
FROM __cg_nodes_IP__cg_rows AS e
WHERE NOT isNull(e.ip)
GROUP BY e.ip
)
SELECT 
      v0.ip AS "src.ip", 
      v2.ip AS "dest.ip"
FROM __cg_nodes_IP AS v0
JOIN zeek.conn_log AS v1 ON v1."id.orig_h" = v0.ip
JOIN __cg_nodes_IP AS v2 ON v1."id.resp_h" = v2.ip
