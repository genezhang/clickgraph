WITH __cg_nodes_IP__cg_rows AS (
SELECT 
      e.`id.orig_h` AS `ip_address`
FROM zeek.dns_log AS e

UNION ALL
SELECT 
      e.`id.resp_h` AS `ip_address`
FROM zeek.dns_log AS e
), 
__cg_nodes_IP AS (
SELECT 
      e.ip_address AS `ip_address`
FROM __cg_nodes_IP__cg_rows AS e
WHERE NOT isnull(e.ip_address)
GROUP BY e.ip_address
)
SELECT 
      v0.ip_address AS `n.ip_address`
FROM __cg_nodes_IP AS v0
WHERE v0.ip_address IS NOT NULL
LIMIT 10
-- column n.ip_address: Value [("n.ip_address", "n.ip_address")]