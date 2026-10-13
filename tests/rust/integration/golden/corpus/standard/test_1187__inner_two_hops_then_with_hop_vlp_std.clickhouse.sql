WITH RECURSIVE with_w1 AS (
SELECT 
      v4.user_id AS "v5__user_id", 
      v4.user_id AS "p2_v5_user_id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.user_follows_test AS v3 ON v3.follower_id = v2.user_id
JOIN test_integration.users_test AS v4 ON v3.followed_id = v4.user_id
WHERE v1.follow_id <> v3.follow_id
), 
vlp_v8_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      v7.user_id AS "id"
FROM with_w1 AS w1
JOIN test_integration.user_follows_test AS v6 ON v6.follower_id = w1.v5__user_id
JOIN test_integration.users_test AS v7 ON v6.followed_id = v7.user_id))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v8_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
)
SELECT 
      w1.p2_v5_user_id AS "c.user_id", 
      v9.user_id AS "b.user_id"
FROM vlp_v8_path AS v8
JOIN test_integration.users_test AS v7 ON v8.start_id = v7.user_id
JOIN test_integration.user_follows_test AS v6 ON v6.followed_id = v7.user_id
JOIN with_w1 AS w1 ON v6.follower_id = w1.v5__user_id
JOIN test_integration.users_test AS v9 ON v8.end_id = v9.user_id
WHERE NOT has(v8.path_edges, v6.follow_id)
