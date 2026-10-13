WITH __cg_nodes_Domain__cg_rows AS (
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
), 
__cg_nodes_IP__cg_rows AS (
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
      anyIfOrNull(CAST(coalesce(e.port__cg2, e.port__cg3), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg2), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg3), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.port__cg2, e.port__cg3)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.port__cg2, e.port__cg3)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), NOT isNull(CAST(coalesce(e.port__cg2, e.port__cg3), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg2), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg3), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.port__cg2, e.port__cg3)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.port__cg2, e.port__cg3)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')))) AS "port"
FROM __cg_nodes_IP__cg_rows AS e
WHERE NOT isNull(e.ip)
GROUP BY e.ip
), 
v2_labels AS (
SELECT 
      'Domain' AS "__cg_label", 
      e.name AS "__cg_id_0", 
      NULL AS "p2_v2_ip", 
      e.name AS "p2_v2_name", 
      NULL AS "p2_v2_port"
FROM __cg_nodes_Domain AS e

UNION ALL
SELECT 
      'IP' AS "__cg_label", 
      e.ip AS "__cg_id_0", 
      e.ip AS "p2_v2_ip", 
      NULL AS "p2_v2_name", 
      e.port AS "p2_v2_port"
FROM __cg_nodes_IP AS e
), 
v1_rels AS (
SELECT 
      'ACCESSED' AS "__cg_type", 
      'IP' AS "__cg_from_label", 
      'IP' AS "__cg_to_label", 
      e.uid AS "__cg_rid_0", 
      e."id.orig_h" AS "__cg_from_0", 
      e."id.resp_h" AS "__cg_to_0", 
      e."id.orig_h" AS "__cg_start_0", 
      e."id.resp_h" AS "__cg_end_0", 
      'IP' AS "__cg_start_label", 
      'IP' AS "__cg_end_label", 
      concat('ACCESSED:IP:IP:', toString(e.uid)) AS "__cg_key"
FROM zeek.conn_log AS e

UNION ALL
SELECT 
      'REQUESTED' AS "__cg_type", 
      'IP' AS "__cg_from_label", 
      'Domain' AS "__cg_to_label", 
      e.uid AS "__cg_rid_0", 
      e."id.orig_h" AS "__cg_from_0", 
      e.query AS "__cg_to_0", 
      e."id.orig_h" AS "__cg_start_0", 
      e.query AS "__cg_end_0", 
      'IP' AS "__cg_start_label", 
      'Domain' AS "__cg_end_label", 
      concat('REQUESTED:IP:Domain:', toString(e.uid)) AS "__cg_key"
FROM zeek.dns_log AS e
)
SELECT 
      v1.__cg_type AS "type(r)", 
      map('elementId', CAST(concat(v2.__cg_label, ':', toString(v2.__cg_id_0), '-'), 'Dynamic'), 'labels', CAST([v2.__cg_label], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('ip', CAST(v2.p2_v2_ip, 'Dynamic'), 'name', CAST(v2.p2_v2_name, 'Dynamic'), 'port', CAST(v2.p2_v2_port, 'Dynamic'))), 'Dynamic')) AS "target"
FROM __cg_nodes_IP AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.ip
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
WHERE v0.ip = '192.168.1.10'
LIMIT 10