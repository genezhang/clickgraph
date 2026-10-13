WITH RECURSIVE __cg_nodes_Airport__cg_rows AS (
SELECT 
      e.origin_code AS `code`, 
      e.origin_city AS `city__cg0`, 
      NULL AS `city__cg1`, 
      e.origin_state AS `state__cg0`, 
      NULL AS `state__cg1`
FROM db_denormalized.flights_denorm AS e

UNION ALL
SELECT 
      e.dest_code AS `code`, 
      NULL AS `city__cg0`, 
      e.dest_city AS `city__cg1`, 
      NULL AS `state__cg0`, 
      e.dest_state AS `state__cg1`
FROM db_denormalized.flights_denorm AS e
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
        array(struct(rel.flight_id, rel.flight_number)) as path_edges
    FROM __cg_nodes_Airport AS start_node
    JOIN db_denormalized.flights_denorm AS rel ON start_node.code = rel.origin_code
    JOIN __cg_nodes_Airport AS end_node ON rel.dest_code = end_node.code
    UNION ALL
    SELECT
        vp.start_id,
        end_node.code as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.code)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.flight_id, rel.flight_number))) as path_edges
    FROM vlp_v1_path vp
    JOIN db_denormalized.flights_denorm AS rel ON vp.end_id = rel.origin_code
    JOIN __cg_nodes_Airport AS end_node ON rel.dest_code = end_node.code
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.flight_id, rel.flight_number))
)
SELECT 
      v0.code AS `a.code`, 
      v2.code AS `b.code`
FROM vlp_v1_path AS v1
JOIN __cg_nodes_Airport AS v0 ON v1.start_id = v0.code
JOIN __cg_nodes_Airport AS v2 ON v1.end_id = v2.code

-- column a.code: Value [("a.code", "a.code")]
-- column b.code: Value [("b.code", "b.code")]