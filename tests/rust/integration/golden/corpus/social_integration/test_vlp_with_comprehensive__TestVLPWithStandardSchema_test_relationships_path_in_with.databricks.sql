WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(to_variant_object(map('elementId', CAST(concat('FOLLOWS:', concat(coalesce(replace(replace(replace(CAST(rel.follow_id AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|'), '\\N')), ':', CAST(rel.follower_id AS STRING), '->', CAST(rel.followed_id AS STRING), '-') AS VARIANT), 'startNodeElementId', CAST(concat('User:', CAST(rel.follower_id AS STRING), '-') AS VARIANT), 'endNodeElementId', CAST(concat('User:', CAST(rel.followed_id AS STRING), '-') AS VARIANT), 'type', CAST('FOLLOWS' AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('follow_date', CAST(rel.follow_date AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))) as path_rel_values,
        array(named_struct('_1', rel.follower_id, '_2', rel.followed_id, '_3', rel.follow_id, '_4', rel.follow_date)) as path_rel_tuples,
        array(rel.follow_id) as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id = 1)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_rel_values, array(to_variant_object(map('elementId', CAST(concat('FOLLOWS:', concat(coalesce(replace(replace(replace(CAST(rel.follow_id AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|'), '\\N')), ':', CAST(rel.follower_id AS STRING), '->', CAST(rel.followed_id AS STRING), '-') AS VARIANT), 'startNodeElementId', CAST(concat('User:', CAST(rel.follower_id AS STRING), '-') AS VARIANT), 'endNodeElementId', CAST(concat('User:', CAST(rel.followed_id AS STRING), '-') AS VARIANT), 'type', CAST('FOLLOWS' AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('follow_date', CAST(rel.follow_date AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))))) as path_rel_values,
        concat(vp.path_rel_tuples, array(named_struct('_1', rel.follower_id, '_2', rel.followed_id, '_3', rel.follow_id, '_4', rel.follow_date))) as path_rel_tuples,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
), 
with_w2 AS (
SELECT 
      v0.user_id AS `v4__user_id`, 
      v0.full_name AS `p2_v4_name`, 
      v2.user_id AS `v5__user_id`, 
      v2.full_name AS `p2_v5_name`, 
      v1.path_rel_tuples AS `v6`, 
      v1.hop_count AS `v7`
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
WHERE (v0.user_id = 1 AND v1.hop_count = 2)
)
SELECT 
      w2.p2_v4_name AS `u1.name`, 
      w2.p2_v5_name AS `u2.name`, 
      array_size(w2.v6) AS `rel_count`
FROM with_w2 AS w2

-- column u1.name: Value [("u1.name", "u1.name")]
-- column u2.name: Value [("u2.name", "u2.name")]
-- column rel_count: Value [("rel_count", "rel_count")]