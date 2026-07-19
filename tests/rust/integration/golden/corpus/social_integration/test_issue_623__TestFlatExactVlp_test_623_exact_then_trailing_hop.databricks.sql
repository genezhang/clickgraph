WITH RECURSIVE vlp_a_b_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(rel.follow_id) as path_edges,
        start_node.full_name as start_name,
        end_node.full_name as end_name
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
        concat(vp.path_edges, array(rel.follow_id)) as path_edges,
        vp.start_name as start_name,
        end_node.full_name as end_name
    FROM vlp_a_b_inner vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE hop_count >= 2
)
SELECT 
      t.start_name AS `a.name`, 
      t.end_name AS `b.name`, 
      c.full_name AS `c.name`
FROM vlp_a_b AS t
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = t.end_id
INNER JOIN test_integration.users_test AS c ON c.user_id = t0.followed_id
LIMIT 10