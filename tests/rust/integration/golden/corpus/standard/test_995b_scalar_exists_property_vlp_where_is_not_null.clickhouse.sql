WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.followed_id
    JOIN test_integration.users_test AS end_node ON rel.follower_id = end_node.user_id
    WHERE (start_node.full_name IS NOT NULL)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.followed_id
    JOIN test_integration.users_test AS end_node ON rel.follower_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
)
SELECT 
      v0.user_id AS "u.user_id"
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.end_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.start_id = v2.user_id
WHERE v2.full_name IS NOT NULL

-- column u.user_id: Value [("u.user_id", "u.user_id")]