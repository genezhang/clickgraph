WITH RECURSIVE with_w1 AS (
SELECT 
      v2.user_id AS "v3__user_id", 
      v2.user_id AS "p2_v3_user_id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
), 
vlp_v6_path AS (
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
      v5.user_id AS "id"
FROM with_w1 AS w1
JOIN test_integration.user_follows_test AS v4 ON v4.follower_id = w1.v3__user_id
JOIN test_integration.users_test AS v5 ON v4.followed_id = v5.user_id))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v6_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
optional_o3 AS (
SELECT 
      v8.follower_id AS "v7__user_id", 
      v9.user_id AS "v9__user_id", 
      v9.user_id AS "p2_v9_user_id"
FROM test_integration.user_follows_test AS v8
JOIN test_integration.users_test AS v9 ON v8.followed_id = v9.user_id
)
SELECT 
      w1.p2_v3_user_id AS "c.user_id", 
      o3.p2_v9_user_id AS "d.user_id"
FROM with_w1 AS w1
JOIN test_integration.user_follows_test AS v4 ON v4.follower_id = w1.v3__user_id
JOIN test_integration.users_test AS v5 ON v4.followed_id = v5.user_id
JOIN vlp_v6_path AS v6 ON v6.start_id = v5.user_id
JOIN test_integration.users_test AS v7 ON v6.end_id = v7.user_id
LEFT JOIN optional_o3 AS o3 ON v7.user_id = o3.v7__user_id
WHERE NOT has(v6.path_edges, v4.follow_id)

-- column c.user_id: Value [("c.user_id", "c.user_id")]
-- column d.user_id: Value [("d.user_id", "d.user_id")]