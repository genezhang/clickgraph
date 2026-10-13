WITH RECURSIVE with_w1 AS (
SELECT 
      v1.Dest AS "v3__code", 
      v1.Dest AS "p2_v3_code"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
), 
__cg_nodes_Airport__cg_rows AS (
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
      arrayElement([anyIfOrNull(CAST(coalesce(e.city__cg0, e.city__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.city__cg0, e.city__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.city__cg0, e.city__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), NOT isNull(CAST(coalesce(e.city__cg0, e.city__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.city__cg0, e.city__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.city__cg0, e.city__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types'))))], 1 + throwIf(uniqUpToIf(1)(cityHash64(CAST(coalesce(e.city__cg0, e.city__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.city__cg0, e.city__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.city__cg0, e.city__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types'))), NOT isNull(CAST(coalesce(e.city__cg0, e.city__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.city__cg0, e.city__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.city__cg0, e.city__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')))) > 1, 'ClickGraph: rows of one :Airport node hold several values of its property city: the property is not the node\'s, or its id columns do not identify it')) AS "city", 
      arrayElement([anyIfOrNull(CAST(coalesce(e.state__cg0, e.state__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.state__cg0, e.state__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.state__cg0, e.state__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), NOT isNull(CAST(coalesce(e.state__cg0, e.state__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.state__cg0, e.state__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.state__cg0, e.state__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types'))))], 1 + throwIf(uniqUpToIf(1)(cityHash64(CAST(coalesce(e.state__cg0, e.state__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.state__cg0, e.state__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.state__cg0, e.state__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types'))), NOT isNull(CAST(coalesce(e.state__cg0, e.state__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.state__cg0, e.state__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.state__cg0, e.state__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')))) > 1, 'ClickGraph: rows of one :Airport node hold several values of its property state: the property is not the node\'s, or its id columns do not identify it')) AS "state"
FROM __cg_nodes_Airport__cg_rows AS e
WHERE NOT isNull(e.code)
GROUP BY e.code
), 
vlp_v8_path AS (
    SELECT 
        start_node.code as start_id,
        end_node.code as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.code, end_node.code] as path_nodes,
        [tuple(rel.flight_id, rel.flight_number)] as path_edges
    FROM __cg_nodes_Airport AS start_node
    JOIN test_integration.flights AS rel ON start_node.code = rel.Origin
    JOIN __cg_nodes_Airport AS end_node ON rel.Dest = end_node.code
    WHERE (start_node.code IN (SELECT DISTINCT 
      v7.code AS "id"
FROM with_w1 AS w1
JOIN test_integration.flights AS v4 ON v4.Origin = w1.v3__code
JOIN test_integration.flights AS v6 ON v6.Origin = v4.Dest
JOIN __cg_nodes_Airport AS v7 ON v6.Dest = v7.code
WHERE v4.Dest IS NOT NULL))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.code as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.code]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.flight_id, rel.flight_number)]) as path_edges
    FROM vlp_v8_path vp
    JOIN test_integration.flights AS rel ON vp.end_id = rel.Origin
    JOIN __cg_nodes_Airport AS end_node ON rel.Dest = end_node.code
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.flight_id, rel.flight_number))
)
SELECT 
      w1.p2_v3_code AS "c.code", 
      v4.Dest AS "m.code", 
      v7.code AS "a.code", 
      v9.code AS "b.code"
FROM vlp_v8_path AS v8
JOIN __cg_nodes_Airport AS v7 ON v8.start_id = v7.code
JOIN test_integration.flights AS v6 ON v6.Dest = v7.code
JOIN test_integration.flights AS v4 ON v6.Origin = v4.Dest
JOIN with_w1 AS w1 ON v4.Origin = w1.v3__code
JOIN __cg_nodes_Airport AS v9 ON v8.end_id = v9.code
WHERE (v4.Dest IS NOT NULL AND (v4.flight_id <> v6.flight_id OR v4.flight_number <> v6.flight_number) AND NOT has(v8.path_edges, tuple(v4.flight_id, v4.flight_number)) AND NOT has(v8.path_edges, tuple(v6.flight_id, v6.flight_number)))

-- column c.code: Value [("c.code", "c.code")]
-- column m.code: Value [("m.code", "m.code")]
-- column a.code: Value [("a.code", "a.code")]
-- column b.code: Value [("b.code", "b.code")]