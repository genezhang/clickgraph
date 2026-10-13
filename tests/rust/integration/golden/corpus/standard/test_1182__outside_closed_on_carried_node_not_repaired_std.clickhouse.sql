WITH RECURSIVE with_w1 AS (
SELECT 
      v2.user_id AS "v3__user_id", 
      v2.user_id AS "p2_v3_user_id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
), 
vlp_v7_path AS (
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
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      w1.v3__user_id AS "id"
FROM with_w1 AS w1))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v7_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.followed_id
    JOIN test_integration.users_test AS end_node ON rel.follower_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, rel.follow_id)
)
SELECT 
      w1.p2_v3_user_id AS "c.user_id", 
      v4.user_id AS "n0.user_id"
FROM vlp_v7_path AS v7
JOIN with_w1 AS w1 ON v7.start_id = w1.v3__user_id
JOIN test_integration.users_test AS v6 ON v7.end_id = v6.user_id
JOIN test_integration.user_follows_test AS v5 ON v5.followed_id = v6.user_id
JOIN test_integration.users_test AS v4 ON v5.follower_id = v4.user_id
WHERE NOT has(v7.path_edges, v5.follow_id)
