WITH RECURSIVE with_c_cte_0 AS (SELECT 
      t0.followed_id AS `p1_c_user_id`
FROM test_integration.users_test AS z
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = z.user_id
), 
vlp_c_n2 AS (
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
    FROM vlp_c_n2 vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
)
SELECT 
      c.p1_c_user_id AS `c.user_id`, 
      t.end_id AS `n2.user_id`
FROM vlp_c_n2 AS t
INNER JOIN with_c_cte_0 AS c ON string(t.start_id) = string(c.p1_c_user_id)
JOIN test_integration.users_test AS n0 ON 1 = 1
INNER JOIN test_integration.user_follows_test AS t1 ON t1.follower_id = n0.user_id AND t1.followed_id = t.start_id
