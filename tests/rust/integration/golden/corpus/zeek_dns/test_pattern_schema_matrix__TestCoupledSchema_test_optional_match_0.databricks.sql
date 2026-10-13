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
      v0.ip_address AS `a.ip_address`, 
      CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS `rel_count`
FROM __cg_nodes_IP AS v0
GROUP BY v0.ip_address

-- column a.ip_address: Value [("a.ip_address", "a.ip_address")]
-- column rel_count: Value [("rel_count", "rel_count")]