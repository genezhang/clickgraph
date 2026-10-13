WITH __cg_nodes_IP__cg_rows AS (
SELECT 
      e."id.orig_h" AS "ip", 
      NULL AS "port__cg2", 
      NULL AS "port__cg3"
FROM zeek.dns_log AS e

UNION ALL
SELECT 
      e."id.resp_h" AS "ip", 
      NULL AS "port__cg2", 
      NULL AS "port__cg3"
FROM zeek.dns_log AS e

UNION ALL
SELECT 
      e."id.orig_h" AS "ip", 
      e."id.orig_p" AS "port__cg2", 
      NULL AS "port__cg3"
FROM zeek.conn_log AS e

UNION ALL
SELECT 
      e."id.resp_h" AS "ip", 
      NULL AS "port__cg2", 
      e."id.resp_p" AS "port__cg3"
FROM zeek.conn_log AS e
), 
__cg_nodes_IP AS (
SELECT 
      e.ip AS "ip", 
      arrayElement([anyIfOrNull(CAST(coalesce(e.port__cg2, e.port__cg3), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg2), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg3), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.port__cg2, e.port__cg3)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.port__cg2, e.port__cg3)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), NOT isNull(CAST(coalesce(e.port__cg2, e.port__cg3), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg2), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg3), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.port__cg2, e.port__cg3)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.port__cg2, e.port__cg3)), 'ClickGraph_property_has_different_types_on_different_labels_or_types'))))], 1 + throwIf(uniqUpToIf(1)(cityHash64(CAST(coalesce(e.port__cg2, e.port__cg3), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg2), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg3), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.port__cg2, e.port__cg3)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.port__cg2, e.port__cg3)), 'ClickGraph_property_has_different_types_on_different_labels_or_types'))), NOT isNull(CAST(coalesce(e.port__cg2, e.port__cg3), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg2), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg3), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.port__cg2, e.port__cg3)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.port__cg2, e.port__cg3)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')))) > 1, 'ClickGraph: rows of one :IP node hold several values of its property port: the property is not the node\'s, or its id columns do not identify it')) AS "port"
FROM __cg_nodes_IP__cg_rows AS e
WHERE NOT isNull(e.ip)
GROUP BY e.ip
), 
__cg_nodes_Domain__cg_rows AS (
SELECT 
      e.query AS "name"
FROM zeek.dns_log AS e
), 
__cg_nodes_Domain AS (
SELECT 
      e.name AS "name"
FROM __cg_nodes_Domain__cg_rows AS e
WHERE NOT isNull(e.name)
GROUP BY e.name
)
SELECT 
      v0.ip AS "src.ip", 
      v0.port AS "src.port", 
      v2.name AS "d.name"
FROM __cg_nodes_IP AS v0
JOIN zeek.dns_log AS v1 ON v1."id.orig_h" = v0.ip
JOIN __cg_nodes_Domain AS v2 ON v1.query = v2.name
LIMIT 1
-- column src: Node { label: "IP" } [("ip", "src.ip"), ("port", "src.port")]
-- column d: Node { label: "Domain" } [("name", "d.name")]