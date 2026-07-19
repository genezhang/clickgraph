WITH RECURSIVE vlp_b_c_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges,
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
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges,
        vp.start_name as start_name,
        end_node.full_name as end_name
    FROM vlp_b_c_inner vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
),
vlp_b_c AS (
    SELECT * FROM vlp_b_c_inner WHERE hop_count >= 2
)
SELECT 
      a.full_name AS "a.name", 
      t.start_name AS "b.name", 
      t.end_name AS "c.name"
FROM vlp_b_c AS t
JOIN test_integration.users_test AS a ON 1 = 1
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id AND t0.followed_id = t.start_id
LIMIT 10