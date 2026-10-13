WITH RECURSIVE with_w1 AS (
SELECT 
      v1.Dest AS `v3__code`, 
      v1.Dest AS `p2_v3_code`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
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
vlp_v7_path AS (
    SELECT 
        start_node.code as start_id,
        end_node.code as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.code, end_node.code) as path_nodes,
        array(struct(rel.flight_id, rel.flight_number)) as path_edges
    FROM __cg_nodes_Airport AS start_node
    JOIN test_integration.flights AS rel ON start_node.code = rel.Dest
    JOIN __cg_nodes_Airport AS end_node ON rel.Origin = end_node.code
    WHERE (start_node.code IN (SELECT DISTINCT 
      w1.v3__code AS `id`
FROM with_w1 AS w1))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.code as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.code)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.flight_id, rel.flight_number))) as path_edges
    FROM vlp_v7_path vp
    JOIN test_integration.flights AS rel ON vp.end_id = rel.Dest
    JOIN __cg_nodes_Airport AS end_node ON rel.Origin = end_node.code
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(rel.flight_id, rel.flight_number))
)
SELECT 
      w1.p2_v3_code AS `c.code`, 
      v5.Origin AS `n0.code`
FROM vlp_v7_path AS v7
JOIN with_w1 AS w1 ON v7.start_id = w1.v3__code
JOIN __cg_nodes_Airport AS v6 ON v7.end_id = v6.code
JOIN test_integration.flights AS v5 ON v5.Dest = v6.code
WHERE (v5.Origin IS NOT NULL AND NOT array_contains(v7.path_edges, struct(v5.flight_id, v5.flight_number)))

-- column c.code: Value [("c.code", "c.code")]
-- column n0.code: Value [("n0.code", "n0.code")]