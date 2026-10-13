WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [tuple(rel.follower_id, rel.followed_id)] as path_edges
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id = 1)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.follower_id, rel.followed_id)]) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.follower_id, rel.followed_id))
), 
with_w2 AS (
SELECT 
      v0.user_id AS "v4__user_id", 
      v0.name AS "p2_v4_name", 
      v2.user_id AS "v5__user_id", 
      v2.name AS "p2_v5_name", 
      v1.hop_count AS "v6"
FROM vlp_v1_path AS v1
JOIN test_integration.users AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
WHERE (v0.user_id = 1 AND v1.hop_count = 2)
)
SELECT 
      w2.p2_v4_name AS "u1.name", 
      w2.p2_v5_name AS "u2.name", 
      w2.v6 AS "path_len"
FROM with_w2 AS w2

-- column u1.name: Value [("u1.name", "u1.name")]
-- column u2.name: Value [("u2.name", "u2.name")]
-- column path_len: Value [("path_len", "path_len")]