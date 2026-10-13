WITH __cg_nodes_IP__cg_rows AS (
SELECT 
      e.`id.orig_h` AS `ip`, 
      NULL AS `port__cg2`, 
      NULL AS `port__cg3`
FROM zeek.dns_log AS e

UNION ALL
SELECT 
      e.`id.resp_h` AS `ip`, 
      NULL AS `port__cg2`, 
      NULL AS `port__cg3`
FROM zeek.dns_log AS e

UNION ALL
SELECT 
      e.`id.orig_h` AS `ip`, 
      e.`id.orig_p` AS `port__cg2`, 
      NULL AS `port__cg3`
FROM zeek.conn_log AS e

UNION ALL
SELECT 
      e.`id.resp_h` AS `ip`, 
      NULL AS `port__cg2`, 
      e.`id.resp_p` AS `port__cg3`
FROM zeek.conn_log AS e
), 
__cg_nodes_IP AS (
SELECT 
      e.ip AS `ip`, 
      any_value(CASE WHEN regexp_replace(regexp_replace(typeof(e.port__cg2), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.port__cg3), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.port__cg2, e.port__cg3) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, true) AS `port`
FROM __cg_nodes_IP__cg_rows AS e
WHERE NOT isnull(e.ip)
GROUP BY e.ip
), 
__cg_nodes_Domain__cg_rows AS (
SELECT 
      e.query AS `name`
FROM zeek.dns_log AS e
), 
__cg_nodes_Domain AS (
SELECT 
      e.name AS `name`
FROM __cg_nodes_Domain__cg_rows AS e
WHERE NOT isnull(e.name)
GROUP BY e.name
)
SELECT 
      v0.ip AS `src.ip`, 
      v0.port AS `src.port`, 
      v2.name AS `d.name`
FROM __cg_nodes_IP AS v0
JOIN zeek.dns_log AS v1 ON v1.`id.orig_h` = v0.ip
JOIN __cg_nodes_Domain AS v2 ON v1.query = v2.name
LIMIT 1
-- column src: Node { label: "IP" } [("ip", "src.ip"), ("port", "src.port")]
-- column d: Node { label: "Domain" } [("name", "d.name")]