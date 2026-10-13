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
), 
optional_o3 AS (
SELECT 
      v1."id.orig_h" AS "v0__ip_address", 
      v1.uid AS "v1__uid", 
      v1."id.orig_h" AS "v1__id.orig_h", 
      v1.query AS "v1__query", 
      v1.query AS "v2__domain_name"
FROM zeek.dns_log AS v1
WHERE v1.query IS NOT NULL
)
SELECT 
      v0.ip_address AS "a.ip_address", 
      count(o3.v1__uid) AS "rel_count"
FROM __cg_nodes_IP AS v0
LEFT JOIN optional_o3 AS o3 ON v0.ip_address = o3.v0__ip_address
GROUP BY v0.ip_address
