WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [map('elementId', CAST(concat('FOLLOWS:', toString(rel.follower_id), '->', toString(rel.followed_id), '-'), 'Dynamic'), 'startNodeElementId', CAST(concat('User:', toString(rel.follower_id), '-'), 'Dynamic'), 'endNodeElementId', CAST(concat('User:', toString(rel.followed_id), '-'), 'Dynamic'), 'type', CAST('FOLLOWS', 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('follow_date', CAST(rel.follow_date, 'Dynamic'))), 'Dynamic'))] as path_rel_values,
        [tuple(rel.follower_id, rel.followed_id, rel.follow_id, rel.follow_date)] as path_rel_tuples,
        [rel.follow_id] as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id = 1)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_rel_values, [map('elementId', CAST(concat('FOLLOWS:', toString(rel.follower_id), '->', toString(rel.followed_id), '-'), 'Dynamic'), 'startNodeElementId', CAST(concat('User:', toString(rel.follower_id), '-'), 'Dynamic'), 'endNodeElementId', CAST(concat('User:', toString(rel.followed_id), '-'), 'Dynamic'), 'type', CAST('FOLLOWS', 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('follow_date', CAST(rel.follow_date, 'Dynamic'))), 'Dynamic'))]) as path_rel_values,
        arrayConcat(vp.path_rel_tuples, [tuple(rel.follower_id, rel.followed_id, rel.follow_id, rel.follow_date)]) as path_rel_tuples,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
with_w2 AS (
SELECT 
      v0.user_id AS "v4__user_id", 
      v0.full_name AS "p2_v4_name", 
      v2.user_id AS "v5__user_id", 
      v2.full_name AS "p2_v5_name", 
      v1.path_rel_tuples AS "v6", 
      v1.hop_count AS "v7"
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
WHERE (v0.user_id = 1 AND v1.hop_count = 2)
)
SELECT 
      w2.p2_v4_name AS "u1.name", 
      w2.p2_v5_name AS "u2.name", 
      length(w2.v6) AS "rel_count"
FROM with_w2 AS w2
