WITH RECURSIVE __cg_nodes_Airport__cg_rows AS (
SELECT 
      e.Origin AS "code", 
      e.OriginCityName AS "city__cg0", 
      NULL AS "city__cg1", 
      e.OriginState AS "state__cg0", 
      NULL AS "state__cg1"
FROM test_integration.flights AS e

UNION ALL
SELECT 
      e.Dest AS "code", 
      NULL AS "city__cg0", 
      e.DestCityName AS "city__cg1", 
      NULL AS "state__cg0", 
      e.DestState AS "state__cg1"
FROM test_integration.flights AS e
), 
__cg_nodes_Airport AS (
SELECT 
      e.code AS "code", 
      anyIfOrNull(CAST(coalesce(e.city__cg0, e.city__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.city__cg0, e.city__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.city__cg0, e.city__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), NOT isNull(CAST(coalesce(e.city__cg0, e.city__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.city__cg0, e.city__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.city__cg0, e.city__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')))) AS "city", 
      anyIfOrNull(CAST(coalesce(e.state__cg0, e.state__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.state__cg0, e.state__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.state__cg0, e.state__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), NOT isNull(CAST(coalesce(e.state__cg0, e.state__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.state__cg0, e.state__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.state__cg0, e.state__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')))) AS "state"
FROM __cg_nodes_Airport__cg_rows AS e
WHERE NOT isNull(e.code)
GROUP BY e.code
), 
v1_both AS (
SELECT 
      e.Dest AS "Dest", 
      e.Origin AS "Origin", 
      e.airline AS "airline", 
      e.arr_time AS "arr_time", 
      e.dep_time AS "dep_time", 
      e.distance_miles AS "distance_miles", 
      e.flight_id AS "flight_id", 
      e.flight_number AS "flight_number", 
      e.Origin AS "__cg_start_0", 
      e.Dest AS "__cg_end_0"
FROM test_integration.flights AS e

UNION ALL
SELECT 
      e.Dest AS "Dest", 
      e.Origin AS "Origin", 
      e.airline AS "airline", 
      e.arr_time AS "arr_time", 
      e.dep_time AS "dep_time", 
      e.distance_miles AS "distance_miles", 
      e.flight_id AS "flight_id", 
      e.flight_number AS "flight_number", 
      e.Dest AS "__cg_start_0", 
      e.Origin AS "__cg_end_0"
FROM test_integration.flights AS e
WHERE e.Origin <> e.Dest
), 
vlp_v1_path AS (
    SELECT 
        start_node.code as start_id,
        end_node.code as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.code, end_node.code] as path_nodes,
        [tuple(rel.flight_id, rel.flight_number)] as path_edges
    FROM __cg_nodes_Airport AS start_node
    JOIN v1_both AS rel ON start_node.code = rel.__cg_start_0
    JOIN __cg_nodes_Airport AS end_node ON rel.__cg_end_0 = end_node.code
    UNION ALL
    SELECT
        vp.start_id,
        end_node.code as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.code]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.flight_id, rel.flight_number)]) as path_edges
    FROM vlp_v1_path vp
    JOIN v1_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN __cg_nodes_Airport AS end_node ON rel.__cg_end_0 = end_node.code
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.flight_id, rel.flight_number))
)
SELECT 
      count(*) AS "count(*)"
FROM vlp_v1_path AS v1
JOIN __cg_nodes_Airport AS v0 ON v1.start_id = v0.code
JOIN __cg_nodes_Airport AS v2 ON v1.end_id = v2.code
JOIN test_integration.flights AS v3 ON v3.Origin = v2.code
WHERE (v3.Dest IS NOT NULL AND NOT has(v1.path_edges, tuple(v3.flight_id, v3.flight_number)))
