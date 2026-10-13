WITH RECURSIVE __cg_nodes_Airport__cg_rows AS (
SELECT 
      e.Origin AS `code`, 
      e.OriginCityName AS `city__cg0`, 
      NULL AS `city__cg1`, 
      e.OriginState AS `state__cg0`, 
      NULL AS `state__cg1`
FROM test_integration.flights AS e

UNION ALL
SELECT 
      e.Dest AS `code`, 
      NULL AS `city__cg0`, 
      e.DestCityName AS `city__cg1`, 
      NULL AS `state__cg0`, 
      e.DestState AS `state__cg1`
FROM test_integration.flights AS e
), 
__cg_nodes_Airport AS (
SELECT 
      e.code AS `code`, 
      any_value(CASE WHEN regexp_replace(regexp_replace(typeof(e.city__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.city__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.city__cg0, e.city__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, true) AS `city`, 
      any_value(CASE WHEN regexp_replace(regexp_replace(typeof(e.state__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.state__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.state__cg0, e.state__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, true) AS `state`
FROM __cg_nodes_Airport__cg_rows AS e
WHERE NOT isnull(e.code)
GROUP BY e.code
), 
vlp_v1_path AS (
    SELECT 
        start_node.code as start_id,
        end_node.code as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.code, end_node.code) as path_nodes,
        array(to_variant_object(map('elementId', CAST(concat('Airport:', CAST(start_node.code AS STRING), '-') AS VARIANT), 'labels', CAST(array('Airport') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('city', CAST(start_node.city AS VARIANT), 'code', CAST(start_node.code AS VARIANT), 'state', CAST(start_node.state AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))), to_variant_object(map('elementId', CAST(concat('Airport:', CAST(end_node.code AS STRING), '-') AS VARIANT), 'labels', CAST(array('Airport') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('city', CAST(end_node.city AS VARIANT), 'code', CAST(end_node.code AS VARIANT), 'state', CAST(end_node.state AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))) as path_node_values,
        array(named_struct('_1', start_node.code, '_2', start_node.city, '_3', start_node.code, '_4', start_node.state), named_struct('_1', end_node.code, '_2', end_node.city, '_3', end_node.code, '_4', end_node.state)) as path_node_tuples,
        array(struct(rel.flight_id, rel.flight_number)) as path_edges
    FROM __cg_nodes_Airport AS start_node
    JOIN test_integration.flights AS rel ON start_node.code = rel.Origin
    JOIN __cg_nodes_Airport AS end_node ON rel.Dest = end_node.code
    WHERE (start_node.code = 'LAX')
    UNION ALL
    SELECT
        vp.start_id,
        end_node.code as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.code)) as path_nodes,
        concat(vp.path_node_values, array(to_variant_object(map('elementId', CAST(concat('Airport:', CAST(end_node.code AS STRING), '-') AS VARIANT), 'labels', CAST(array('Airport') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('city', CAST(end_node.city AS VARIANT), 'code', CAST(end_node.code AS VARIANT), 'state', CAST(end_node.state AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))))) as path_node_values,
        concat(vp.path_node_tuples, array(named_struct('_1', end_node.code, '_2', end_node.city, '_3', end_node.code, '_4', end_node.state))) as path_node_tuples,
        concat(vp.path_edges, array(struct(rel.flight_id, rel.flight_number))) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.flights AS rel ON vp.end_id = rel.Origin
    JOIN __cg_nodes_Airport AS end_node ON rel.Dest = end_node.code
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.flight_id, rel.flight_number))
), 
with_w4 AS (
SELECT 
      v0.code AS `v4__code`, 
      v0.city AS `p2_v4_city`, 
      v2.code AS `v5__code`, 
      v2.city AS `p2_v5_city`, 
      concat(array(named_struct('_1', v0.code, '_2', v0.city, '_3', v0.code, '_4', v0.state)), filter(v1.path_node_tuples, (__x, __i) -> __i > 0)) AS `v6`, 
      v1.hop_count AS `v7`
FROM vlp_v1_path AS v1
JOIN __cg_nodes_Airport AS v0 ON v1.start_id = v0.code
JOIN __cg_nodes_Airport AS v2 ON v1.end_id = v2.code
WHERE (v0.code = 'LAX' AND v1.hop_count = 2)
)
SELECT 
      w4.p2_v4_city AS `a.city`, 
      w4.p2_v5_city AS `b.city`, 
      array_size(w4.v6) AS `node_count`
FROM with_w4 AS w4
ORDER BY w4.p2_v5_city ASC NULLS LAST
LIMIT 5
-- column a.city: Value [("a.city", "a.city")]
-- column b.city: Value [("b.city", "b.city")]
-- column node_count: Value [("node_count", "node_count")]