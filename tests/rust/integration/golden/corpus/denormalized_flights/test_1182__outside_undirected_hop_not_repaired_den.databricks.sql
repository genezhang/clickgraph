WITH RECURSIVE with_w1 AS (
SELECT 
      v1.Dest AS `v3__code`, 
      v1.Dest AS `p2_v3_code`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
), 
v4_both AS (
SELECT 
      e.Dest AS `Dest`, 
      e.Origin AS `Origin`, 
      e.airline AS `airline`, 
      e.arr_time AS `arr_time`, 
      e.dep_time AS `dep_time`, 
      e.distance_miles AS `distance_miles`, 
      e.flight_id AS `flight_id`, 
      e.flight_number AS `flight_number`, 
      e.Origin AS `__cg_start_0`, 
      e.Dest AS `__cg_end_0`
FROM test_integration.flights AS e

UNION ALL
SELECT 
      e.Dest AS `Dest`, 
      e.Origin AS `Origin`, 
      e.airline AS `airline`, 
      e.arr_time AS `arr_time`, 
      e.dep_time AS `dep_time`, 
      e.distance_miles AS `distance_miles`, 
      e.flight_id AS `flight_id`, 
      e.flight_number AS `flight_number`, 
      e.Dest AS `__cg_start_0`, 
      e.Origin AS `__cg_end_0`
FROM test_integration.flights AS e
WHERE e.Origin <> e.Dest
), 
__cg_nodes_Airport__cg_rows AS (
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
vlp_v6_path AS (
    SELECT 
        start_node.code as start_id,
        end_node.code as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.code, end_node.code) as path_nodes,
        array(struct(rel.flight_id, rel.flight_number)) as path_edges
    FROM __cg_nodes_Airport AS start_node
    JOIN test_integration.flights AS rel ON start_node.code = rel.Origin
    JOIN __cg_nodes_Airport AS end_node ON rel.Dest = end_node.code
    WHERE (start_node.code IN (SELECT DISTINCT 
      v5.code AS `id`
FROM with_w1 AS w1
JOIN v4_both AS v4 ON v4.__cg_start_0 = w1.v3__code
JOIN __cg_nodes_Airport AS v5 ON v4.__cg_end_0 = v5.code))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.code as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.code)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.flight_id, rel.flight_number))) as path_edges
    FROM vlp_v6_path vp
    JOIN test_integration.flights AS rel ON vp.end_id = rel.Origin
    JOIN __cg_nodes_Airport AS end_node ON rel.Dest = end_node.code
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.flight_id, rel.flight_number))
)
SELECT 
      w1.p2_v3_code AS `c.code`, 
      v7.code AS `b.code`
FROM vlp_v6_path AS v6
JOIN __cg_nodes_Airport AS v5 ON v6.start_id = v5.code
JOIN v4_both AS v4 ON v4.__cg_end_0 = v5.code
JOIN with_w1 AS w1 ON v4.__cg_start_0 = w1.v3__code
JOIN __cg_nodes_Airport AS v7 ON v6.end_id = v7.code
WHERE NOT array_contains(v6.path_edges, struct(v4.flight_id, v4.flight_number))
