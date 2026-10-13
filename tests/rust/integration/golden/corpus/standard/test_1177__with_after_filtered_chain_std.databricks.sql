WITH RECURSIVE vlp_v3_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(rel.follow_id) as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      v2.user_id AS `id`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
WHERE v0.full_name = 'Alice Smith'))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_v3_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
), 
with_w2 AS (
SELECT 
      count(*) AS `v5`
FROM vlp_v3_path AS v3
JOIN test_integration.users_test AS v2 ON v3.start_id = v2.user_id
JOIN test_integration.user_follows_test AS v1 ON v1.followed_id = v2.user_id
JOIN test_integration.users_test AS v0 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v4 ON v3.end_id = v4.user_id
WHERE (NOT array_contains(v3.path_edges, v1.follow_id) AND v0.full_name = 'Alice Smith')
)
SELECT 
      w2.v5 AS `k`
FROM with_w2 AS w2
