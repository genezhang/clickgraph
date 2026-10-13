WITH RECURSIVE with_w1 AS (
SELECT 
      v3.Dest AS `v5__code`, 
      v3.Dest AS `p2_v5_code`
FROM test_integration.flights AS v1
JOIN test_integration.flights AS v3 ON v3.Origin = v1.Dest
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v3.Dest IS NOT NULL AND (v1.flight_id <> v3.flight_id OR v1.flight_number <> v3.flight_number))
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
      CASE WHEN count(DISTINCT CASE WHEN regexp_replace(regexp_replace(typeof(e.city__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.city__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.city__cg0, e.city__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END) > 1 THEN raise_error('ClickGraph: rows of one :Airport node hold several values of its property city: the property is not the node\'s, or its id columns do not identify it') ELSE any_value(CASE WHEN regexp_replace(regexp_replace(typeof(e.city__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.city__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.city__cg0, e.city__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, true) END AS `city`, 
      CASE WHEN count(DISTINCT CASE WHEN regexp_replace(regexp_replace(typeof(e.state__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.state__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.state__cg0, e.state__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END) > 1 THEN raise_error('ClickGraph: rows of one :Airport node hold several values of its property state: the property is not the node\'s, or its id columns do not identify it') ELSE any_value(CASE WHEN regexp_replace(regexp_replace(typeof(e.state__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.state__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.state__cg0, e.state__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, true) END AS `state`
FROM __cg_nodes_Airport__cg_rows AS e
WHERE NOT isnull(e.code)
GROUP BY e.code
), 
vlp_v8_path AS (
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
      v7.code AS `id`
FROM with_w1 AS w1
JOIN test_integration.flights AS v6 ON v6.Origin = w1.v5__code
JOIN __cg_nodes_Airport AS v7 ON v6.Dest = v7.code))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.code as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.code)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.flight_id, rel.flight_number))) as path_edges
    FROM vlp_v8_path vp
    JOIN test_integration.flights AS rel ON vp.end_id = rel.Origin
    JOIN __cg_nodes_Airport AS end_node ON rel.Dest = end_node.code
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.flight_id, rel.flight_number))
)
SELECT 
      w1.p2_v5_code AS `c.code`, 
      v9.code AS `b.code`
FROM vlp_v8_path AS v8
JOIN __cg_nodes_Airport AS v7 ON v8.start_id = v7.code
JOIN test_integration.flights AS v6 ON v6.Dest = v7.code
JOIN with_w1 AS w1 ON v6.Origin = w1.v5__code
JOIN __cg_nodes_Airport AS v9 ON v8.end_id = v9.code
WHERE NOT array_contains(v8.path_edges, struct(v6.flight_id, v6.flight_number))

-- column c.code: Value [("c.code", "c.code")]
-- column b.code: Value [("b.code", "b.code")]