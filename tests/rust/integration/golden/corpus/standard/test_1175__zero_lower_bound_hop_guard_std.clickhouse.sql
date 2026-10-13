WITH RECURSIVE vlp_v3_path AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id] as path_nodes,
        (
            SELECT arraySlice([__seed_edge.follow_id], 1, 0)
            FROM test_integration.user_follows_test AS __seed_edge
            LIMIT 1
        ) as path_edges
    FROM test_integration.users_test AS start_node
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      v2.user_id AS "id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v3_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
)
SELECT 
      count(*) AS "count(*)"
FROM vlp_v3_path AS v3
JOIN test_integration.users_test AS v2 ON v3.start_id = v2.user_id
JOIN test_integration.user_follows_test AS v1 ON v1.followed_id = v2.user_id
JOIN test_integration.users_test AS v0 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v4 ON v3.end_id = v4.user_id
WHERE NOT has(v3.path_edges, v1.follow_id)
