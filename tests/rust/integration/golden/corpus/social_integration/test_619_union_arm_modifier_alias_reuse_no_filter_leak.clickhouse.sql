WITH RECURSIVE vlp_v1_path AS (
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
    WHERE (start_node.user_id = 5)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
with_w2 AS (
SELECT 
      v2.user_id AS "__cg_w2_c0", 
      v2.user_id AS "__cg_w2_o0"
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
WHERE v0.user_id = 5
ORDER BY v2.user_id ASC
LIMIT 2), 
with_w3 AS (
SELECT 
      v4.user_id AS "__cg_w3_c0"
FROM test_integration.users_test AS v4
WHERE v4.user_id = 6
), 
with_w4 AS (
SELECT 
      CAST(w2.__cg_w2_c0, 'Dynamic') AS "__cg_c0", 
      0 AS "__cg_arm", 
      row_number() OVER (ORDER BY w2.__cg_w2_o0 ASC) AS "__cg_row"
FROM with_w2 AS w2

UNION ALL
SELECT 
      CAST(w3.__cg_w3_c0, 'Dynamic') AS "__cg_c0", 
      1 AS "__cg_arm", 
      0 AS "__cg_row"
FROM with_w3 AS w3
)
SELECT 
      w4.__cg_c0 AS "id"
FROM with_w4 AS w4
ORDER BY w4.__cg_arm ASC, w4.__cg_row ASC
