WITH __cg_nodes_IP__cg_rows AS (
SELECT 
      e.orig_h AS "ip", 
      e.orig_p AS "port__cg0", 
      NULL AS "port__cg1"
FROM test_zeek.conn_log AS e

UNION ALL
SELECT 
      e.resp_h AS "ip", 
      NULL AS "port__cg0", 
      e.resp_p AS "port__cg1"
FROM test_zeek.conn_log AS e
), 
__cg_nodes_IP AS (
SELECT 
      e.ip AS "ip", 
      anyIfOrNull(CAST(coalesce(e.port__cg0, e.port__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.port__cg0, e.port__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.port__cg0, e.port__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), NOT isNull(CAST(coalesce(e.port__cg0, e.port__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.port__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.port__cg0, e.port__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.port__cg0, e.port__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')))) AS "port"
FROM __cg_nodes_IP__cg_rows AS e
WHERE NOT isNull(e.ip)
GROUP BY e.ip
)
SELECT 
      count(DISTINCT v0.ip) AS "cnt"
FROM __cg_nodes_IP AS v0

-- column cnt: Value [("cnt", "cnt")]