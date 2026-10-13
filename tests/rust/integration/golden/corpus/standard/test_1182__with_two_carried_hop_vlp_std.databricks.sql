WITH RECURSIVE with_w1 AS (
SELECT 
      v2.user_id AS `v3__user_id`, 
      v2.user_id AS `p2_v3_user_id`, 
      v0.user_id AS `v4__user_id`, 
      v0.user_id AS `p2_v4_user_id`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
), 
vlp_v7_path AS (
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
      v6.user_id AS `id`
FROM with_w1 AS w1
JOIN test_integration.user_follows_test AS v5 ON v5.follower_id = w1.v3__user_id
JOIN test_integration.users_test AS v6 ON v5.followed_id = v6.user_id))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_v7_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
)
SELECT 
      w1.p2_v4_user_id AS `z.user_id`, 
      w1.p2_v3_user_id AS `c.user_id`, 
      v8.user_id AS `b.user_id`
FROM vlp_v7_path AS v7
JOIN test_integration.users_test AS v6 ON v7.start_id = v6.user_id
JOIN test_integration.user_follows_test AS v5 ON v5.followed_id = v6.user_id
JOIN with_w1 AS w1 ON v5.follower_id = w1.v3__user_id
JOIN test_integration.users_test AS v8 ON v7.end_id = v8.user_id
WHERE NOT array_contains(v7.path_edges, v5.follow_id)

-- column z.user_id: Value [("z.user_id", "z.user_id")]
-- column c.user_id: Value [("c.user_id", "c.user_id")]
-- column b.user_id: Value [("b.user_id", "b.user_id")]