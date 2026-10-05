WITH RECURSIVE vlp_b_a AS (
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
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_b_a vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
)
SELECT 
      t.end_id AS `a.user_id`, 
      t0.follower_id AS `d.user_id`
FROM vlp_b_a AS t
INNER JOIN test_integration.user_follows_test AS t1 ON t1.followed_id = t.start_id
INNER JOIN test_integration.user_follows_test AS t0 ON t0.followed_id = t1.follower_id
WHERE t0.follow_id <> t1.follow_id
