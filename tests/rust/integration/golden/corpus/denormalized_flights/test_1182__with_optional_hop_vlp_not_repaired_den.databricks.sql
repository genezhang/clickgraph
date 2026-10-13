WITH RECURSIVE with_w1 AS (
SELECT 
      v1.Dest AS `v3__code`, 
      v1.Dest AS `p2_v3_code`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
), 
optional_d2 AS (
SELECT DISTINCT 
      w1.v3__code AS `v3__code`
FROM with_w1 AS w1
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
FROM optional_d2 AS d2
JOIN test_integration.flights AS v4 ON v4.Origin = d2.v3__code
JOIN __cg_nodes_Airport AS v5 ON v4.Dest = v5.code))
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
), 
optional_o3 AS (
SELECT 
      d2.v3__code AS `v3__code`, 
      v5.code AS `v5__code`, 
      v5.code AS `p2_v5_code`, 
      v7.code AS `v7__code`, 
      v7.code AS `p2_v7_code`
FROM vlp_v6_path AS v6
JOIN __cg_nodes_Airport AS v5 ON v6.start_id = v5.code
JOIN test_integration.flights AS v4 ON v4.Dest = v5.code
JOIN optional_d2 AS d2 ON v4.Origin = d2.v3__code
JOIN __cg_nodes_Airport AS v7 ON v6.end_id = v7.code
WHERE NOT array_contains(v6.path_edges, struct(v4.flight_id, v4.flight_number))
)
SELECT 
      w1.p2_v3_code AS `c.code`, 
      o3.p2_v5_code AS `a.code`, 
      o3.p2_v7_code AS `b.code`
FROM with_w1 AS w1
LEFT JOIN optional_o3 AS o3 ON w1.v3__code = o3.v3__code

-- column c.code: Value [("c.code", "c.code")]
-- column a.code: Value [("a.code", "a.code")]
-- column b.code: Value [("b.code", "b.code")]