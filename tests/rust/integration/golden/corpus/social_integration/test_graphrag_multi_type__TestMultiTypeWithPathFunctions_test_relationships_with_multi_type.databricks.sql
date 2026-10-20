WITH RECURSIVE v2_labels AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`
FROM test_integration.users_test AS e
), 
vlp_v1_nodes AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`, 
      concat('Post', ':', CAST(e.post_id AS STRING)) AS `__cg_key`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      concat('User', ':', CAST(e.user_id AS STRING)) AS `__cg_key`
FROM test_integration.users_test AS e
), 
v1_rels AS (
SELECT 
      'AUTHORED' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.author_id AS `__cg_rid_0`, 
      e.post_id AS `__cg_rid_1`, 
      e.author_id AS `__cg_from_0`, 
      e.post_id AS `__cg_to_0`, 
      e.author_id AS `__cg_start_0`, 
      e.post_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('AUTHORED:User:Post:', to_json(named_struct('_1', CAST(e.author_id AS STRING), '_2', CAST(e.post_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`, 
      to_variant_object(map('elementId', CAST(concat('AUTHORED:', CAST(e.author_id AS STRING), '->', CAST(e.post_id AS STRING), '-') AS VARIANT), 'startNodeElementId', CAST(concat('User:', CAST(e.author_id AS STRING), '-') AS VARIANT), 'endNodeElementId', CAST(concat('Post:', CAST(e.post_id AS STRING), '-') AS VARIANT), 'type', CAST('AUTHORED' AS VARIANT), 'properties', CAST(to_variant_object(map_filter(CAST(map() AS MAP<STRING, VARIANT>), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))) AS `__cg_value`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'FOLLOWS' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'User' AS `__cg_to_label`, 
      e.follow_id AS `__cg_rid_0`, 
      NULL AS `__cg_rid_1`, 
      e.follower_id AS `__cg_from_0`, 
      e.followed_id AS `__cg_to_0`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'User' AS `__cg_end_label`, 
      concat('FOLLOWS:User:User:', CAST(e.follow_id AS STRING)) AS `__cg_key`, 
      to_variant_object(map('elementId', CAST(concat('FOLLOWS:', concat(coalesce(replace(replace(replace(CAST(e.follow_id AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|'), '\\N')), ':', CAST(e.follower_id AS STRING), '->', CAST(e.followed_id AS STRING), '-') AS VARIANT), 'startNodeElementId', CAST(concat('User:', CAST(e.follower_id AS STRING), '-') AS VARIANT), 'endNodeElementId', CAST(concat('User:', CAST(e.followed_id AS STRING), '-') AS VARIANT), 'type', CAST('FOLLOWS' AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('follow_date', CAST(e.follow_date AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))) AS `__cg_value`
FROM test_integration.user_follows_test AS e
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes, CAST(array() AS ARRAY<VARIANT>) AS path_rel_values
    FROM vlp_v1_nodes AS start_node
    WHERE (start_node.__cg_label = 'User') AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      'User' AS `label`, 
      v0.user_id AS `id`
FROM test_integration.users_test AS v0
WHERE v0.user_id = 1))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes, concat(vp.path_rel_values, array(rel.__cg_value)) AS path_rel_values
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 1
      AND NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes, path_rel_values
    FROM vlp_v1_trails
    WHERE hop_count >= 1
)
SELECT 
      v1.path_rel_values AS `rels`, 
      try_element_at(array(v2.__cg_label), 2) AS `node_type`
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN v2_labels AS v2 ON v1.end_label = v2.__cg_label AND v1.end_id = v2.__cg_id_0
WHERE v0.user_id = 1
LIMIT 10
-- column rels: Graph(List(Relationship)) [("rels", "rels")]
-- column node_type: Value [("node_type", "node_type")]